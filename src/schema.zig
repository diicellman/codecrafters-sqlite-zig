const std = @import("std");
const Page = @import("page.zig").Page;

pub const TableSchema = struct {
    root_page: usize,

    pub fn fromRecord(record: Page.Cell.Record) !TableSchema {
        const root_page = switch (record.values[3]) {
            .Integer => |v| @as(usize, @intCast(@as(u32, @intCast(v)))),
            else => return error.InvalidSchemaFormat,
        };

        return TableSchema{
            .root_page = root_page,
        };
    }
};

pub fn findTable(page: Page, table_name: []const u8) ?TableSchema {
    for (page.cells) |cell| {
        if (cell.payload.values.len < 5) continue;

        const type_val = cell.payload.values[0];
        const name_val = cell.payload.values[2];

        const is_table = switch (type_val) {
            .Text => |t| std.mem.eql(u8, t, "table"),
            else => false,
        };
        if (!is_table) continue;

        const matches = switch (name_val) {
            .Text => |n| std.mem.eql(u8, n, table_name),
            else => false,
        };
        if (!matches) continue;

        return TableSchema.fromRecord(cell.payload) catch return null;
    }

    return null;
}
