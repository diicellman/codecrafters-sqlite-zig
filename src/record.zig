const std = @import("std");
const Varint = @import("varint.zig");

pub const RecordPayload = union(enum) {
    Null: void,
    Blob: []const u8,
    Text: []const u8,
    Int: isize,
    Float: []const u8,

    pub fn compare(self: RecordPayload, other: RecordPayload) std.math.Order {
        return switch (self) {
            .Null => .lt,
            .Int => switch (other) {
                .Null => .gt,
                .Int => std.math.order(self.Int, other.Int),
                .Float => .lt,
                .Text => .lt,
                .Blob => .lt,
            },
            .Float => .lt,
            .Text => switch (other) {
                .Null => .gt,
                .Int => .gt,
                .Float => .gt,
                .Text => std.ascii.orderIgnoreCase(self.Text, other.Text),
                .Blob => .lt,
            },
            .Blob => .lt,
        };
    }
};

pub const Record = struct {
    payloads: std.ArrayList(RecordPayload),

    pub fn init(allocator: std.mem.Allocator, reader: std.fs.File.Reader) !Record {
        const header_start_pos = try reader.context.getPos();
        const record_header_size = try Varint.parse(reader);
        var cur_serial_type_pos = try reader.context.getPos();
        const payloads_start_pos = header_start_pos + record_header_size;
        var cur_payload_pos = payloads_start_pos;

        const payloads = std.ArrayList(RecordPayload).init(allocator);
        var record = Record{ .payloads = payloads };
        errdefer record.deinit(allocator);

        while (cur_serial_type_pos != payloads_start_pos) {
            const serial_type = try Varint.parse(reader);
            // save current position. come back here after reading the payload.
            cur_serial_type_pos = try reader.context.getPos();
            try reader.context.seekTo(cur_payload_pos);

            const payload = switch (serial_type) {
                0 => RecordPayload{ .Null = {} },
                1 => RecordPayload{ .Int = try reader.readInt(i8, .big) },
                2 => RecordPayload{ .Int = try reader.readInt(i16, .big) },
                3 => RecordPayload{ .Int = try reader.readInt(i24, .big) },
                4 => RecordPayload{ .Int = try reader.readInt(i32, .big) },
                5 => RecordPayload{ .Int = try reader.readInt(i48, .big) },
                6 => RecordPayload{ .Int = try reader.readInt(i64, .big) },
                7 => blk: {
                    const buf = try allocator.alloc(u8, 8);
                    _ = try reader.read(buf);
                    break :blk RecordPayload{ .Float = buf };
                },
                8...11 => RecordPayload{ .Null = {} },
                else => |n| blk: {
                    const size: usize = if (n % 2 == 0) (n - 12) / 2 else (n - 13) / 2;
                    const buf = try allocator.alloc(u8, size);
                    _ = try reader.read(buf);
                    if (n % 2 == 0) {
                        break :blk RecordPayload{ .Blob = buf };
                    } else {
                        break :blk RecordPayload{ .Text = buf };
                    }
                },
            };

            // save current position. come back here in the next iteration.
            cur_payload_pos = try reader.context.getPos();
            try record.payloads.append(payload);
            try reader.context.seekTo(cur_serial_type_pos);
        }

        return record;
    }

    pub fn deinit(self: *Record, allocator: std.mem.Allocator) void {
        for (self.payloads.items) |payload| {
            switch (payload) {
                .Float, .Text, .Blob => |v| {
                    allocator.free(v);
                },
                else => {},
            }
        }
        self.payloads.deinit();
    }

    pub fn debug(self: Record) void {
        for (self.payloads.items) |payload| {
            std.debug.print("{any}\n", .{payload});
        }
    }
};
