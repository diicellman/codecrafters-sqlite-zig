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
