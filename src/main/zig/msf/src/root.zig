const std = @import("std");
const testing = std.testing;

test "bogus test in root" {
    try testing.expect(3 + 7 == 10);
}
