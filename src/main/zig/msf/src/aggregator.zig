const std = @import("std");
const sort = std.sort;
const fs = std.fs;

pub const Aggregator = struct {
    allocator: std.mem.Allocator,
    data: std.AutoHashMap(u64, Aggregate),
    names: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) !Aggregator {
        var map = std.AutoHashMap(u64, Aggregate).init(allocator);
        try map.ensureTotalCapacity(10_000);
        return Aggregator{
            .allocator = allocator,
            .data = map,
            .names = try std.ArrayList([]const u8).initCapacity(allocator, 10_000),
        };
    }
    pub fn deinit(self: *Aggregator) void {
        self.data.deinit();
        for (self.names.items) |name| {
            self.allocator.free(name);
        }
        self.names.deinit();
    }

    pub fn process(self: *Aggregator, filename: []const u8, writer: anytype) !void {
        try self.process_chunk(filename, 0, 0);
        try self.writeTo(writer);
    }

    pub fn process_chunk(self: *Aggregator, filename: []const u8, start: u64, end: u64) !void {
        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();
        try file.seekTo(start);

        const MAX_LINE = 128;
        var buffer: [MAX_LINE]u8 = undefined;
        var reader = file.reader();
        var curr: u64 = 0;
        var first: bool = true;

        while (try reader.readUntilDelimiterOrEof(&buffer, '\n')) |line| {
            if (end != 0 and curr > end) {
                break;
            }
            curr += line.len + 1;
            if (first and start != 0) {
                first = false;
                continue;
            }

            try self.add(line);
        }
    }

    pub fn add(self: *Aggregator, line: []const u8) !void {
        const measure = try Measurement.parse(line);
        const val = measure.value;
        // now we hash the name and look it up on data
        const hashId = hashName(measure.name);
        const gop = try self.data.getOrPut(hashId);
        if (gop.found_existing) {
            gop.value_ptr.*.add(val);
        } else {
            gop.value_ptr.*.init(val);
            const name = try self.allocator.dupe(u8, measure.name);
            try self.names.append(name);
        }
    }

    pub fn writeTo(self: Aggregator, writer: anytype) !void {
        sortNames(self.names.items);
        try writer.writeAll("{");
        for (self.names.items, 0..) |name, idx| {
            const agg = self.data.get(hashName(name)).?;
            if (idx > 0) {
                try writer.writeAll(", ");
            }
            try writer.print("{s}=", .{name});
            try agg.writeTo(writer);
        }
        try writer.writeAll("}\n");
    }
};

inline fn hashName(name: []const u8) u64 {
    const seed: u64 = 1337;
    return std.hash.Wyhash.hash(seed, name);
}

const Measurement = struct {
    name: []const u8,
    value: i32,
    pub fn parse(line: []const u8) !Measurement {
        var it = std.mem.splitBackwardsScalar(u8, line, ';');
        const value = it.first();
        const name = line[0..(line.len - value.len - 1)];
        const vali: i32 = parsei32(value);
        return .{ .name = name, .value = vali };
    }
};

const Aggregate = struct {
    max: i32,
    min: i32,
    sum: i32,
    count: usize,

    pub fn init(self: *Aggregate, val: i32) void {
        self.max = val;
        self.min = val;
        self.sum = val;
        self.count = 1;
    }

    pub fn add(self: *Aggregate, value: i32) void {
        if (value > self.max) {
            self.max = value;
        }
        if (value < self.min) {
            self.min = value;
        }
        self.sum += value;
        self.count += 1;
    }

    pub fn merge(self: *Aggregate, other: Aggregate) void {
        if (other.max > self.max) {
            self.max = other.max;
        }
        if (other.min < self.min) {
            self.min = other.min;
        }
        self.sum += other.sum;
        self.count += other.count;
    }

    pub fn avg(self: Aggregate) f32 {
        const sumf = @as(f32, @floatFromInt(self.sum));
        var countf = @as(f32, @floatFromInt(self.count));
        countf *= 10.0;
        const v = sumf / countf;
        return round(v);
    }

    pub fn writeTo(self: Aggregate, writer: anytype) !void {
        const min = @as(f32, @floatFromInt(self.min)) / 10.0;
        const max = @as(f32, @floatFromInt(self.max)) / 10.0;

        try writer.print(
            "{d:3.1}/{d:3.1}/{d:3.1}",
            .{ round(min), round(self.avg()), round(max) },
        );
    }
};

inline fn round(val: f32) f32 {
    @setFloatMode(.optimized);
    const v = std.math.floor((val + 0.05) * 10.0);
    return v / 10.0;
}

inline fn parsei32(str: []const u8) i32 {
    var num: i32 = 0;
    var sign: i32 = 1;
    var i: usize = 0;
    while (i < str.len) : (i += 1) {
        if (i == 0 and str[i] == '-') {
            sign = -1;
        } else if (str[i] == '.') {
            continue; // skip
        } else {
            num *= 10;
            num += @as(i32, str[i] - '0');
        }
    }
    return num * sign;
}

fn sortNames(items: [][]const u8) void {
    std.sort.block([]const u8, items, {}, (struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }).lessThan);
}

test "Measurement.parse" {
    const val = try Measurement.parse("PRESTAÇÃO;42.1");
    try std.testing.expectEqual(@as(i32, 421), val.value);
    try std.testing.expectEqualSlices(u8, "PRESTAÇÃO", val.name);
}

test "basic aggregator" {
    const testing = std.testing;
    const alloc = std.testing.allocator;
    var aggregator = try Aggregator.init(alloc);
    defer aggregator.deinit();

    try aggregator.add("Loc1;25.0");
    try aggregator.add("Loc2;30.0");
    try aggregator.add("Loc1;20.0");
    try aggregator.add("Loc2;35.0");
    try aggregator.add("Loc3;15.0");

    const expectedData = "{Loc1=20.0/22.5/25.0, Loc2=30.0/32.5/35.0, Loc3=15.0/15.0/15.0}\n";

    var buffer: [100]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buffer);
    try aggregator.writeTo(fbs.writer());
    try testing.expectEqualStrings(expectedData, fbs.getWritten());
}

test "simple test with file" {
    const baseDir = "/home/miguel/play/1brc/src/test/resources/samples/";
    const filename = baseDir ++ "measurements-1.txt";
    const outputFilename = baseDir ++ "measurements-1.out";

    try testSingleFile(filename, outputFilename);
}

test "test ALL files" {
    const alloc = std.testing.allocator;
    _ = alloc;
    const start_path = "../../../../../test/resources/samples";
    _ = start_path;
    //const baseDir = try fs.cwd().realpathAlloc(alloc, start_path);

    //defer alloc.free(baseDir);
    const baseDir = "/home/miguel/play/1brc/src/test/resources/samples/";

    const dir = try std.fs.openDirAbsolute(baseDir, .{ .iterate = true });

    var it = dir.iterate();
    while (try it.next()) |entry| {
        std.debug.print("walk: {s}\n", .{entry.name});
        if (entry.kind != .file) {
            continue;
        }
        if (!std.mem.endsWith(u8, entry.name, ".txt")) {
            continue;
        }
        var buf: [fs.MAX_PATH_BYTES]u8 = undefined;
        const file_path = try dir.realpath(entry.name, &buf);

        var tmp: [100]u8 = undefined;
        const expectedOutputFilename = try std.fmt.bufPrint(
            &tmp,
            "{s}.out",
            .{file_path[0..(file_path.len - 4)]},
        );

        testSingleFile(file_path[0..], expectedOutputFilename[0..]) catch |err| {
            std.debug.print(
                "Error: on files: \ninput {s} \nexpectedOutput: {s}\n",
                .{ file_path, expectedOutputFilename },
            );
            return err;
        };
    }
}

fn testSingleFile(filename: []const u8, expectedOutputFilename: []const u8) !void {
    const testing = std.testing;
    const alloc = std.testing.allocator;
    const output_max_len = 512;
    var bufExpected: [output_max_len]u8 = undefined;
    var buffer: [output_max_len]u8 = undefined;
    var output = std.io.fixedBufferStream(&buffer);

    var aggregator = try Aggregator.init(alloc);
    defer aggregator.deinit();

    // run
    try aggregator.process(filename, output.writer());

    // read expected output file
    var file = try std.fs.openFileAbsolute(expectedOutputFilename, .{ .mode = .read_only });
    defer file.close();
    const bytes_read = try file.readAll(&bufExpected);
    const outputExpected = bufExpected[0..bytes_read];

    try testing.expectEqualStrings(outputExpected, output.getWritten());
}
