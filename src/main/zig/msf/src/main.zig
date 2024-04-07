const std = @import("std");
const Aggregator = @import("aggregator.zig").Aggregator;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    const args = try std.process.argsAlloc(arena.allocator());
    const filename = args[1];
    if (args.len < 2) {
        std.log.err("usage: {s} <filename> <worker_count>", .{args[0]});
        return;
    }
    const worker_count: u32 = if (args.len >= 3)
        try std.fmt.parseInt(u32, args[2], 10)
    else
        10;

    std.log.info("opening {s} with {d} workers", .{ filename, worker_count });

    var agg = try Aggregator.init(arena.allocator());
    defer agg.deinit();
    agg.process(filename, stdout) catch |err| {
        std.log.err("failed to process file: {s}", .{@errorName(err)});
    };
}
