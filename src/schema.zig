const std = @import("std");
const Page = @import("page.zig").Page;

pub const TableSchema = struct {
    root_page: usize,
    sql: []const u8,

    pub fn fromRecord(record: Page.Cell.Record) !TableSchema {
        const root_page = switch (record.values[3]) {
            .Integer => |v| @as(usize, @intCast(@as(u32, @intCast(v)))),
            else => return error.InvalidSchemaFormat,
        };

        const sql = switch (record.values[4]) {
            .Text => |t| t,
            else => return error.InvalidSchemaFormat,
        };

        return TableSchema{
            .root_page = root_page,
            .sql = sql,
        };
    }

    pub fn findColumnIndex(self: TableSchema, column_name: []const u8) !usize {
        const open_paren = std.mem.indexOf(u8, self.sql, "(") orelse return error.InvalidCreateTable;
        const close_paren = std.mem.lastIndexOf(u8, self.sql, ")") orelse return error.InvalidCreateTable;

        const columns_part = self.sql[open_paren + 1 .. close_paren];

        var col_index: usize = 0;
        var it = std.mem.splitSequence(u8, columns_part, ",");
        while (it.next()) |col_def| {
            const trimmed = std.mem.trim(u8, col_def, " \t\n\r");

            var col_parts = std.mem.splitSequence(u8, trimmed, " ");
            if (col_parts.next()) |col_name| {
                if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, col_name, " \t\n\r\""), column_name)) {
                    return col_index;
                }
            }
            col_index += 1;
        }

        return error.ColumnNotFound;
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
