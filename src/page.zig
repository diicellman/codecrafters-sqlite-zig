const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList(u16);
const AnyReader = std.io.AnyReader;
const Reader = std.fs.File.Reader;

pub const Page = struct {
    pub const Type = enum(u8) {
        inter_index = 0x02,
        inter_table = 0x05,
        leaf_index = 0x0A,
        leaf_table = 0x0D,
    };

    pub const Header = struct {
        page_type: Type,
        free_block_offset: u16,
        cell_count: u16,
        cell_content_offset: u16,
        fragmented_free_bytes_count: u8,
        right_most_pointer: ?u32,

        pub fn read(reader: Reader) Header {
            const page_type = reader.readEnum(Type, .big) catch undefined;
            const free_block_offset = reader.readInt(u16, .big) catch undefined;
            const cell_count = reader.readInt(u16, .big) catch undefined;
            const cell_content_offset = reader.readInt(u16, .big) catch undefined;
            const fragmented_free_bytes_count = reader.readInt(u8, .big) catch undefined;
            var right_most_pointer: ?u32 = null;

            if (page_type == .inter_index or page_type == .inter_table) {
                right_most_pointer = reader.readInt(u32, .big) catch undefined;
            }

            return .{
                .page_type = page_type,
                .free_block_offset = free_block_offset,
                .cell_count = cell_count,
                .cell_content_offset = cell_content_offset,
                .fragmented_free_bytes_count = fragmented_free_bytes_count,
                .right_most_pointer = right_most_pointer,
            };
        }
    };

    pub const Cell = struct {
        const Varint = struct {
            const Byte = packed struct {
                value: u7,
                shift: u1,
            };

            fn read(reader: AnyReader) u64 {
                var result: u64 = 0;
                for (0..9) |_| {
                    const byte: Byte = @bitCast(reader.readByte() catch undefined);
                    result = (result << 7) | byte.value;
                    if (byte.shift == 0) {
                        break;
                    }
                }
                return result;
            }
        };

        const Record = struct {
            pub const Value = union(enum) {
                Null: void,
                Text: []const u8,
                Integer: isize,
            };
            allocator: Allocator,
            values: []const Value,

            fn read(allocator: Allocator, reader: Reader) Record {
                const record_start = reader.context.getPos() catch undefined;
                const record_header_size = Varint.read(reader.any());

                if (record_header_size == 0) {
                    return .{
                        .allocator = allocator,
                        .values = &[_]Value{},
                    };
                }

                if (record_header_size > 1000) {
                    return .{
                        .allocator = allocator,

                        .values = &[_]Value{},
                    };
                }

                const types_start = reader.context.getPos() catch undefined;
                const values_start = record_start + record_header_size;

                var types_pos = types_start;
                var values_pos = values_start;
                var values = std.ArrayList(Value).init(allocator);

                while (true) {
                    if (types_pos == values_start) {
                        break;
                    }
                    _ = reader.context.seekTo(types_pos) catch undefined;

                    const serial_type = Varint.read(reader.any());

                    types_pos = reader.context.getPos() catch undefined;
                    _ = reader.context.seekTo(values_pos) catch undefined;

                    const value: Value = switch (serial_type) {
                        0 => .{ .Null = {} },
                        1 => .{ .Integer = reader.readInt(i8, .big) catch undefined },
                        2 => .{ .Integer = reader.readInt(i16, .big) catch undefined },
                        3 => .{ .Integer = reader.readInt(i24, .big) catch undefined },
                        4 => .{ .Integer = reader.readInt(i32, .big) catch undefined },
                        5 => .{ .Integer = reader.readInt(i48, .big) catch undefined },
                        6 => .{ .Integer = reader.readInt(i64, .big) catch undefined },
                        7 => blk: {
                            var bytes: [8]u8 = undefined;
                            _ = reader.readAll(&bytes) catch undefined;
                            break :blk .{ .Integer = @bitCast(reader.readInt(i64, .big) catch undefined) };
                        },
                        8 => .{ .Integer = 0 },
                        9 => .{ .Integer = 1 },
                        10...11 => .{ .Null = {} },
                        else => blk: {
                            const t: usize = if (serial_type & 1 == 1) 13 else 12;
                            const s: usize = (serial_type - t) >> 1;
                            var v = std.ArrayList(u8).initCapacity(allocator, s) catch undefined;
                            _ = v.addManyAt(0, s) catch undefined;
                            _ = reader.read(v.items) catch undefined;
                            break :blk .{ .Text = v.items };
                        },
                    };

                    _ = values.append(value) catch undefined;
                    values_pos = reader.context.getPos() catch undefined;
                }

                return .{
                    .allocator = allocator,
                    .values = values.items,
                };
            }

            fn deinit(self: *Record) void {
                for (self.values) |value| {
                    switch (value) {
                        .Text => self.allocator.free(value.Text),
                        else => {},
                    }
                }
                self.allocator.free(self.values);
                self.* = undefined;
            }
        };

        allocator: Allocator,
        payload_size: u64,
        row_id: u64,
        payload: Record,
        left_child_page: ?u32,

        pub fn TableLeafCell(allocator: Allocator, reader: Reader) Cell {
            const payload_size = Varint.read(reader.any());
            const row_id = Varint.read(reader.any());
            const payload = Record.read(allocator, reader);
            return .{
                .allocator = allocator,
                .payload_size = payload_size,
                .row_id = row_id,
                .payload = payload,
                .left_child_page = null,
            };
        }

        pub fn TableInternalCell(allocator: Allocator, reader: Reader) Cell {
            const left_child = reader.readInt(u32, .big) catch undefined;

            const payload_size = Varint.read(reader.any());

            const row_id = Varint.read(reader.any());

            const payload = Record.read(allocator, reader);

            return .{
                .allocator = allocator,
                .payload_size = payload_size,
                .row_id = row_id,
                .payload = payload,
                .left_child_page = left_child,
            };
        }

        pub fn deinit(self: *Cell) void {
            self.payload.deinit();
            self.* = undefined;
        }
    };
    allocator: Allocator,
    header: Header,
    cells: []const Cell,
    right_most_pointer: ?u32,

    pub fn read(allocator: Allocator, reader: Reader) Page {
        var page_start_pos = reader.context.getPos() catch undefined;

        if (page_start_pos == 100) {
            page_start_pos = 0;
        }

        const header = Header.read(reader);

        const cell_offsets = readCellOffsets(allocator, reader, header.cell_count);
        defer allocator.free(cell_offsets);

        var cells = std.ArrayList(Cell).initCapacity(allocator, header.cell_count) catch undefined;

        for (0..cell_offsets.len) |i| {
            const offset = cell_offsets[i];
            // std.debug.print("Reading cell {d} at offset {d}\n", .{ i, offset });

            _ = reader.context.seekTo(page_start_pos + offset) catch undefined;
            const cell = switch (header.page_type) {
                .inter_table => Cell.TableInternalCell(allocator, reader),
                .leaf_table => Cell.TableLeafCell(allocator, reader),
                else => unreachable,
            };
            _ = cells.append(cell) catch undefined;
        }

        return .{
            .allocator = allocator,
            .header = header,
            .cells = cells.items,
            .right_most_pointer = header.right_most_pointer,
        };
    }

    pub fn deinit(self: *Page) void {
        for (0..self.cells.len) |i| {
            var cell = self.cells[i];
            cell.deinit();
        }
        self.allocator.free(self.cells);
    }
    fn readCellOffsets(allocator: Allocator, reader: Reader, count: u16) []const u16 {
        var offsets = ArrayList.initCapacity(allocator, count) catch undefined;
        var i: usize = 0;
        while (i < count) {
            const offset = reader.readInt(u16, .big) catch undefined;
            _ = offsets.append(offset) catch undefined;
            i += 1;
        }
        return offsets.items;
    }
};
