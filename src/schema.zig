const std = @import("std");
const Page = @import("page.zig").Page;
const SQL = @import("sql_parser.zig");

pub const SchemaRow = struct {
    tbl_name: [:0]const u8,
    rootpage: isize,
    sql: [:0]const u8,
    statement: SQL.Statement,
};

const SchemaError = error{
    InvalidPage,
};

pub const Schema = struct {
    rows: []SchemaRow,

    pub fn init(allocator: std.mem.Allocator, reader: std.fs.File.Reader, page_size: u64) !Schema {
        try reader.context.seekTo(0);
        var page = try Page.init(allocator, reader, page_size);
        defer page.deinit(allocator);

        var rows = try std.ArrayList(SchemaRow).initCapacity(allocator, page.cells.tbl_leaf.len);
        errdefer {
            for (rows.items) |row| {
                allocator.free(row.tbl_name);
                allocator.free(row.sql);
            }
            rows.deinit();
        }

        for (page.cells.tbl_leaf) |cell| {
            const record = cell.payload;
            const tbl_name = switch (record.payloads.items[2]) {
                .Text => |v| v,
                else => return SchemaError.InvalidPage,
            };
            const rootpage = switch (record.payloads.items[3]) {
                .Int => |v| v,
                else => return SchemaError.InvalidPage,
            };
            const sql = switch (record.payloads.items[4]) {
                .Text => |v| v,
                else => return SchemaError.InvalidPage,
            };
            const duped = try allocator.dupeZ(u8, sql);
            const row = SchemaRow{
                .tbl_name = try allocator.dupeZ(u8, tbl_name),
                .rootpage = rootpage,
                .sql = duped,
                .statement = try SQL.Statement.init(allocator, duped),
            };
            try rows.append(row);
        }
        return .{ .rows = try rows.toOwnedSlice() };
    }

    pub fn deinit(self: *Schema, allocator: std.mem.Allocator) void {
        for (0..self.rows.len) |i| {
            var row = self.rows[i];
            allocator.free(row.tbl_name);
            allocator.free(row.sql);
            row.statement.deinit(allocator);
        }
        allocator.free(self.rows);
    }

    pub fn debug(self: Schema) void {
        for (self.rows) |row| {
            std.debug.print("tbl_name: {s}, rootpage: {d}\n", .{ row.tbl_name, row.rootpage });
            std.debug.print("sql: {s}\n", .{row.sql});
            row.statement.debug();
        }
    }
};

test "read schema" {
    var file = try std.fs.cwd().openFile("companies.db", .{});
    defer file.close();
    const allocator = std.testing.allocator;
    const reader = file.reader();
    try reader.context.seekTo(16);
    const page_size = try reader.readInt(u16, .big);
    var schema = try Schema.init(allocator, reader, page_size);
    defer schema.deinit(allocator);
    schema.debug();
    std.debug.print("page size: {}\n", .{page_size});
}
