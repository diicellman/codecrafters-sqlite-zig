const std = @import("std");

const Byte = packed struct {
    value: u7,
    shift: u1,
};

pub fn parse(reader: std.fs.File.Reader) !u64 {
    var result: u64 = 0;
    for (0..9) |_| {
        const byte: Byte = @bitCast(try reader.readByte());
        result = (result << 7) | byte.value;
        if (byte.shift == 0) {
            break;
        }
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
