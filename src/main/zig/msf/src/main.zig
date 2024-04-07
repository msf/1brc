const std = @import("std");
const Aggregator = @import("aggregator.zig").Aggregator;

pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});

    const stdout = std.io.getStdOut().writer();
    defer stdout.flush();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var args = std.process.argsWithoutAllocator(arena);
    _ = args.skip();
    const filename = try args.next(arena);
    const worker_count = try std.fmt.parseInt(usize, try args.next(arena), 10) orelse 10;
    std.log.info("opening {s} with {d} workers", .{ filename, worker_count });

    var agg = Aggregator.init(arena);
    agg.process(filename, stdout) catch |err| {
        std.log.err("failed to process file: {s}", .{@errorName(err)});
    };
}
