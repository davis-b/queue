// Intended for use with Zig version 0.8

const std = @import("std");

pub const QueueError = error{
    /// Tried to add to a queue that is full.
    FullQueue,

    /// Tried to take from an empty queue.
    EmptyQueue,

    /// ThreadsafeQueue's "wait" function call has timed out.
    TimedOut,
};

/// Threadsafe wrapper around our FIFO Queue
pub fn ThreadsafeQueue(comptime T: type, comptime maxItems: usize) type {
    return struct {
        const Self = @This();
        queue: Queue(T, maxItems),
        mutex: std.Thread.Mutex,
        wait_limiter: std.Thread.Mutex,
        waiter: std.Thread.StaticResetEvent,

        pub fn init() Self {
            return Self{
                .queue = Queue(T, maxItems).init(),
                .mutex = std.Thread.Mutex{},
                .wait_limiter = std.Thread.Mutex{},
                .waiter = std.Thread.StaticResetEvent{},
            };
        }

        pub fn put(self: *Self, item: T) QueueError!void {
            const held = self.mutex.acquire();
            defer held.release();
            if (!waiter_is_set(&self.waiter)) self.waiter.set();
            return self.queue.put(item);
        }

        pub fn take(self: *Self) QueueError!T {
            const held = self.mutex.acquire();
            defer held.release();
            return self.queue.take();
        }

        pub fn count(self: *Self) usize {
            const held = self.mutex.acquire();
            defer held.release();
            return self.queue.count();
        }

        pub fn copyItems(self: *Self) [maxItems]?T {
            const held = self.mutex.acquire();
            defer held.release();
            return self.queue.copyItems();
        }

        /// Waits until Queue is not empty.
        /// Timeout is in nanoseconds.
        /// Funtion is exclusive to ThreadsafeQueue.
        /// Can be called by multiple threads at the same time.
        ///  Threads beyond the first will wait on a mutex before
        ///  waiting on a reset event.
        pub fn wait(self: *Self, timeout: ?u64) QueueError!T {
            const wait_held = self.wait_limiter.acquire();
            defer wait_held.release();
            while (true) {
                const result = self.take() catch null;
                if (result) |r| {
                    return r;
                }
                if (timeout) |t| {
                    switch (self.waiter.timedWait(t)) {
                        .timed_out => return QueueError.TimedOut,
                        .event_set => {},
                    }
                } else {
                    self.waiter.wait();
                }
                const held = self.mutex.acquire();
                self.waiter.reset();
                held.release();
            }
        }
    };
}

fn waiter_is_set(waiter: *std.Thread.StaticResetEvent) bool {
    const waiters = @atomicLoad(u32, &waiter.impl.waiters, .Acquire);
    const WAKE = 1 << 0;
    const WAIT = 1 << 1;
    return (waiters == WAKE);
}

/// FIFO queue that is statically allocated at comptime.
pub fn Queue(comptime T: type, comptime maxItems: usize) type {
    return struct {
        const Self = @This();
        items: [maxItems]?T,
        takePos: usize,
        putPos: usize,

        pub fn init() Self {
            return Self{
                .items = [_]?T{null} ** maxItems,
                .takePos = 0,
                .putPos = 0,
            };
        }

        pub fn put(self: *Self, item: T) QueueError!void {
            try self.assertCanPut();
            self.items[self.nextPut()] = item;
        }

        pub fn take(self: *Self) QueueError!T {
            try self.assertCanTake();
            const n = self.nextTake();
            defer self.items[n] = null;
            return self.items[n].?;
        }

        fn nextPut(self: *Self) usize {
            self.putPos = self.next(self.putPos);
            return self.putPos;
        }

        fn nextTake(self: *Self) usize {
            self.takePos = self.next(self.takePos);
            return self.takePos;
        }

        fn next(self: *Self, i: usize) usize {
            var result: usize = i;
            result += 1;
            if (result >= maxItems) result = 0;
            return result;
        }

        fn assertCanTake(self: *Self) QueueError!void {
            //if (self.count() == 0) return QueueError.EmptyQueue;
            if (self.items[self.next(self.takePos)] == null) return QueueError.EmptyQueue;
        }

        fn assertCanPut(self: *Self) QueueError!void {
            //if (self.count() == maxItems) return QueueError.FullQueue;
            if (self.items[self.next(self.putPos)] != null) return QueueError.FullQueue;
        }

        /// Returns number of filled slots in Queue
        pub fn count(self: *Self) usize {
            //    var n: usize = 0;
            //    for (self.items) |i| {
            //        if (i != null) n += 1;
            //    }
            //    return n;
            if (self.putPos > self.takePos) {
                return self.putPos - self.takePos;
            }
            if (self.putPos == self.takePos) {
                if (maxItems == 0) return 0;
                // occurs when queue is full
                if (self.items[self.putPos] != null) return maxItems;
                return 0;
            }
            // self.takePos > self.putPos because putPos has circled around the array
            return (self.putPos + maxItems) - self.takePos;
        }

        /// returns an in order copy of our internal array
        /// in order meaning in the order we would .get() them
        pub fn copyItems(self: *Self) [maxItems]?T {
            var items = [_]?T{null} ** maxItems;
            var item: ?T = null;
            var index: usize = 0;
            var queueIndex: usize = self.next(self.takePos);
            while (true) {
                item = self.items[queueIndex];
                if (item == null or index == maxItems) break;
                items[index] = item;
                queueIndex = self.next(queueIndex);
                index += 1;
            }
            return items;
        }
    };
}

test "happy path" {
    @setEvalBranchQuota(10000);
    const types = [_]type{ u8, i8, u3 };
    inline for (types) |T| {
        comptime var testingCounts = [_]T{0} ** std.math.maxInt(T);
        inline for (testingCounts) |itemCount| {
            //        var q = Queue(T, itemCount).init();
            var q = ThreadsafeQueue(T, itemCount).init();
            var arr = [_]T{0} ** itemCount;
            // set each elem's value to its index
            for (arr) |_, index| arr[index] = @intCast(T, index);
            // put items into queue in order
            for (arr) |i| try q.put(i);
            // take them out
            for (arr) |expected| expect(try q.take() == expected);
            expect(q.count() == 0);
        }
    }
}

test "put too many items" {
    const itemCount = 5;
    const T = u8;
    //var q = Queue(T, itemCount).init();
    var q = ThreadsafeQueue(T, itemCount).init();
    var arr = [_]T{0} ** itemCount;
    for (arr) |_, index| arr[index] = @intCast(T, index);
    for (arr) |i| try q.put(i);
    expectError(QueueError.FullQueue, q.put(100));
    _ = try q.take();
    try q.put(10);
    expectError(QueueError.FullQueue, q.put(10));
}

test "take too many items" {
    const itemCount = 5;
    const T = u8;
    var q = Queue(T, itemCount).init();

    try q.put(1);
    _ = try q.take();
    _ = expectError(QueueError.EmptyQueue, q.take());
}

test "circular behaviour" {
    const itemCount = 3;
    const T = u8;
    //var q = Queue(T, itemCount).init();
    var q = ThreadsafeQueue(T, itemCount).init();
    try q.put(1);
    expect(q.count() == 1);
    try q.put(2);
    expect(q.count() == 2);
    expect(q.take() catch unreachable == 1);
    expect(q.count() == 1);
    try q.put(3);
    expect(q.count() == 2);
    expect(q.take() catch unreachable == 2);
    expect(q.count() == 1);
    try q.put(4);
    expect(q.count() == 2);
    expect(q.take() catch unreachable == 3);
    expect(q.count() == 1);
    expect(q.take() catch unreachable == 4);
    expect(q.count() == 0);
    try q.put(1);
    expect(q.count() == 1);
    expect(q.take() catch unreachable == 1);
    expect(q.count() == 0);
    try q.put(2);
    expect(q.count() == 1);
    try q.put(3);
    expect(q.count() == 2);
    try q.put(4);
    expect(q.count() == 3);
    expectError(QueueError.FullQueue, q.put(100));
    expect(q.count() == 3);
    expect(q.take() catch unreachable == 2);
    expect(q.count() == 2);
    expect(q.take() catch unreachable == 3);
    expect(q.count() == 1);
    try q.put(5);
    expect(q.count() == 2);
    expect(q.take() catch unreachable == 4);
    expect(q.take() catch unreachable == 5);
    expect(q.count() == 0);
}

test "copy items" {
    const itemCount = 3;
    const T = u8;
    var q = Queue(T, itemCount).init();
    var items = [_]?T{null} ** itemCount;
    items = q.copyItems();
    for (items) |i| expect(i == null);

    try q.put(10);
    try q.put(11);
    try q.put(12);
    items = q.copyItems();
    expect(items[0].? == 10);
    expect(items[1].? == 11);
    expect(items[2].? == 12);

    _ = try q.take();
    try q.put(13);
    _ = try q.take();
    try q.put(14);
    items = q.copyItems();
    expect(items[0].? == 12);
    expect(items[1].? == 13);
    expect(items[2].? == 14);
}

test "Optional type" {
    var q = Queue(?u8, 10).init();

    const a: ?u8 = 1;
    const b: ?u8 = null;
    const c: ?u8 = 2;
    try q.put(a);
    expectEqual(@as(usize, 1), q.count());
    try q.put(b);
    expectEqual(@as(usize, 2), q.count());
    try q.put(c);
    expectEqual(@as(usize, 3), q.count());

    expectEqual(@as(?u8, 1), try q.take());
    expectEqual(@as(?u8, null), try q.take());
    expectEqual(@as(?u8, 2), try q.take());

    for (q.items) |_| {
        try q.put(null);
    }
    expectEqual(@as(usize, q.items.len), q.count());
    expectError(QueueError.FullQueue, q.put(null));
    expectError(QueueError.FullQueue, q.put(10));
}

test "Threadsafe waiting" {
    var q = ThreadsafeQueue(u8, 10).init();

    try q.put(10);
    expectEqual(@as(u8, 10), try q.take());

    var thread = try std.Thread.spawn(testDelayedPut, &q);
    const timer_start = std.time.milliTimestamp();
    const wait_result = try q.wait(null);
    const time_taken = std.time.milliTimestamp() - timer_start;
    expectEqual(@as(u8, 4), wait_result);

    // Wait should be around 300 ms
    expect(time_taken > 150);
    expect(time_taken < 450);

    thread.wait();

    expectError(QueueError.EmptyQueue, q.take());
    expectEqual(false, waiter_is_set(&q.waiter));
    expectError(QueueError.TimedOut, q.wait(5 * std.time.ns_per_ms));
}

fn testDelayedPut(queue: *ThreadsafeQueue(u8, 10)) void {
    std.time.sleep(300 * std.time.ns_per_ms);
    queue.put(@as(u8, 4)) catch @panic("Test administration error");
}

test "Timed wait" {
    var q = ThreadsafeQueue(u8, 10).init();
    // wait is in nanoseconds
    expectError(QueueError.TimedOut, q.wait(100));

    const expected: u8 = 33;
    try q.put(expected);
    expectEqual(expected, try q.wait(1));

    expectError(QueueError.TimedOut, q.wait(100));
}

test "Multiple waiting threads" {
    var q = ThreadsafeQueue(u8, 10).init();
    //    try q.put(1);
    var threads: [3]*std.Thread = undefined;
    for (threads) |*thread| {
        thread.* = try std.Thread.spawn(testWait, &q);
    }
    std.time.sleep(5 * std.time.ns_per_ms);

    try q.put(1);
    try q.put(1);
    try q.put(1);

    for (threads) |thread| {
        thread.wait();
    }
}

fn testWait(queue: *ThreadsafeQueue(u8, 10)) void {
    const result = queue.wait(100 * std.time.ns_per_ms) catch @panic("Queue.wait timed out");
}

fn expectEqual(expected: anytype, actual: @TypeOf(expected)) void {
    std.testing.expectEqual(expected, actual) catch unreachable;
}
fn expectError(expected_error: anyerror, actual_error_union: anytype) void {
    std.testing.expectError(expected_error, actual_error_union) catch unreachable;
}
fn expect(ok: bool) void {
    std.testing.expect(ok) catch unreachable;
}
