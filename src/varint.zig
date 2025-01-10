const std = @import("std");

pub fn parse(reader: std.fs.File.Reader) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;

    while (shift < 64) : (shift += 7) {
        const byte = try reader.readByte();
        result |= @as(u64, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) break;
    }

    return result;
}

test "parse varint" {
    var file = try std.fs.cwd().createFile(
        "test.bin",
        .{ .read = true },
    );
    defer file.close();

    const test_cases = [_]struct {
        bytes: []const u8,
        expected: u64,
    }{
        .{ .bytes = &[_]u8{0x01}, .expected = 1 },
        .{ .bytes = &[_]u8{ 0x81, 0x01 }, .expected = 129 },
        .{ .bytes = &[_]u8{ 0xFF, 0x01 }, .expected = 255 },
    };

    for (test_cases) |case| {
        try file.seekTo(0);
        try file.writeAll(case.bytes);
        try file.seekTo(0);
        const result = try parse(file.reader());
        try std.testing.expectEqual(case.expected, result);
    }
}
