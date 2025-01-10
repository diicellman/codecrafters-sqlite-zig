const std = @import("std");
const Allocator = std.mem.Allocator;
const Record = @import("record.zig").Record;
const Varint = @import("varint.zig");

pub const TableLeafCell = struct {
    payload_size: u64,
    key: u64, // rowid
    payload: Record,
    overflow_page_num: ?u32,

    pub fn init(allocator: Allocator, reader: std.fs.File.Reader, page_size: u64) !TableLeafCell {
        const payload_size = try Varint.parse(reader);
        const rowid = try Varint.parse(reader);
        const payload = try Record.init(allocator, reader);
        const has_overflow = payload_size >= (page_size - 35);
        const overflow_page_num = if (has_overflow) try reader.readInt(u32, .big) else null;
        return .{
            .payload_size = payload_size,
            .key = rowid,
            .payload = payload,
            .overflow_page_num = overflow_page_num,
        };
    }

    pub fn deinit(self: *TableLeafCell, allocator: Allocator) void {
        self.payload.deinit(allocator);
    }
};

pub const TableInteriorCell = struct {
    left_page_num: u32,
    key: u64,

    pub fn init(reader: std.fs.File.Reader) !TableInteriorCell {
        const left_page_num = try reader.readInt(u32, .big);
        const key = try Varint.parse(reader);
        return .{
            .left_page_num = left_page_num,
            .key = key,
        };
    }

    pub fn debug(self: TableInteriorCell) void {
        std.debug.print("left pointer: {}, key: {}\n", .{ self.left_page_num, self.key });
    }
};

pub const IndexLeafCell = struct {
    payload_size: u64,
    payload: Record,
    overflow_page_num: ?u32,

    pub fn init(allocator: Allocator, reader: std.fs.File.Reader, page_size: u64) !IndexLeafCell {
        const payload_size = try Varint.parse(reader);
        const payload = try Record.init(allocator, reader);
        const has_overflow = payload_size >= (page_size - 35);
        const overflow_page_num = if (has_overflow) try reader.readInt(u32, .big) else null;
        return .{
            .payload_size = payload_size,
            .payload = payload,
            .overflow_page_num = overflow_page_num,
        };
    }

    pub fn deinit(self: *IndexLeafCell, allocator: Allocator) void {
        self.payload.deinit(allocator);
    }
};

pub const IndexInteriorCell = struct {
    left_page_num: u32,
    payload_size: u64,
    payload: Record,
    overflow_page_num: ?u32,

    pub fn init(allocator: Allocator, reader: std.fs.File.Reader, page_size: u64) !IndexInteriorCell {
        const left_page_num = try reader.readInt(u32, .big);
        const payload_size = try Varint.parse(reader);
        const payload = try Record.init(allocator, reader);
        const has_overflow = payload_size >= (page_size - 35);
        const overflow_page_num = if (has_overflow) try reader.readInt(u32, .big) else null;
        return .{
            .left_page_num = left_page_num,
            .payload_size = payload_size,
            .payload = payload,
            .overflow_page_num = overflow_page_num,
        };
    }

    pub fn deinit(self: *IndexInteriorCell, allocator: Allocator) void {
        self.payload.deinit(allocator);
    }
};

const PageType = enum(u8) {
    tbl_leaf = 0x0d,
    tbl_interior = 0x05,
    idx_leaf = 0x0a,
    idx_interior = 0x02,
};

pub const Cells = union(PageType) {
    tbl_leaf: []TableLeafCell,
    tbl_interior: []TableInteriorCell,
    idx_leaf: []IndexLeafCell,
    idx_interior: []IndexInteriorCell,

    pub fn init(
        allocator: Allocator,
        reader: std.fs.File.Reader,
        page_type: PageType,
        page_size: u64,
        offsets: []u64,
    ) !Cells {
        const num_cells = offsets.len;
        switch (page_type) {
            .tbl_leaf => {
                const cells = try allocator.alloc(TableLeafCell, num_cells);
                errdefer allocator.free(cells);

                for (offsets, 0..) |offset, i| {
                    try reader.context.seekTo(offset);
                    cells[i] = try TableLeafCell.init(allocator, reader, page_size);
                }

                return Cells{ .tbl_leaf = cells };
            },
            .tbl_interior => {
                const cells = try allocator.alloc(TableInteriorCell, num_cells);
                errdefer allocator.free(cells);

                for (offsets, 0..) |offset, i| {
                    try reader.context.seekTo(offset);
                    cells[i] = try TableInteriorCell.init(reader);
                }

                return Cells{ .tbl_interior = cells };
            },
            .idx_leaf => {
                const cells = try allocator.alloc(IndexLeafCell, num_cells);
                errdefer allocator.free(cells);

                for (offsets, 0..) |offset, i| {
                    try reader.context.seekTo(offset);
                    cells[i] = try IndexLeafCell.init(allocator, reader, page_size);
                }

                return Cells{ .idx_leaf = cells };
            },
            .idx_interior => {
                const cells = try allocator.alloc(IndexInteriorCell, num_cells);
                errdefer allocator.free(cells);

                for (offsets, 0..) |offset, i| {
                    try reader.context.seekTo(offset);
                    cells[i] = try IndexInteriorCell.init(allocator, reader, page_size);
                }

                return Cells{ .idx_interior = cells };
            },
        }
    }

    pub fn deinit(self: *Cells, allocator: Allocator) void {
        switch (self.*) {
            .tbl_leaf => |cells| {
                for (cells) |*cell| {
                    cell.deinit(allocator);
                }
                allocator.free(cells);
            },
            .tbl_interior => |cells| {
                allocator.free(cells);
            },
            .idx_leaf => |cells| {
                for (cells) |*cell| {
                    cell.deinit(allocator);
                }
                allocator.free(cells);
            },
            .idx_interior => |cells| {
                for (cells) |*cell| {
                    cell.deinit(allocator);
                }
                allocator.free(cells);
            },
        }
    }
};

pub const Page = struct {
    cells: Cells,
    right_pointer: ?u32,

    pub fn init(allocator: Allocator, reader: std.fs.File.Reader, page_size: u64) !Page {
        const start_pos = try reader.context.getPos();
        const header_start_pos = if (start_pos == 0) 100 else start_pos;

        try reader.context.seekTo(header_start_pos);
        const page_type = try reader.readEnum(PageType, .big);
        try reader.context.seekTo(header_start_pos + 3);
        const num_cells = try reader.readInt(u16, .big);

        var cell_pointers = try allocator.alloc(u64, num_cells);
        defer allocator.free(cell_pointers);

        try reader.context.seekTo(header_start_pos + 8);
        var right_pointer: ?u32 = null;

        switch (page_type) {
            .tbl_interior, .idx_interior => {
                right_pointer = try reader.readInt(u32, .big);
                try reader.context.seekTo(header_start_pos + 12);
            },
            else => {},
        }

        for (0..cell_pointers.len) |i| {
            cell_pointers[i] = start_pos + try reader.readInt(u16, .big);
        }

        const cells = try Cells.init(
            allocator,
            reader,
            page_type,
            page_size,
            cell_pointers,
        );

        return Page{ .cells = cells, .right_pointer = right_pointer };
    }

    pub fn deinit(self: *Page, allocator: Allocator) void {
        self.cells.deinit(allocator);
    }

    pub fn debug(self: Page) void {
        switch (self.cells) {
            .tbl_leaf => |cells| {
                for (cells) |cell| {
                    std.debug.print("{any}\n", .{cell});
                }
            },
            .tbl_interior => |cells| {
                for (cells) |cell| {
                    std.debug.print("{any}\n", .{cell});
                }
            },
            .idx_leaf => |cells| {
                for (cells) |cell| {
                    std.debug.print("{any}\n", .{cell});
                }
            },
            .idx_interior => |cells| {
                for (cells) |cell| {
                    std.debug.print("{any}\n", .{cell});
                }
            },
        }
    }
};
