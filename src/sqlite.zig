const std = @import("std");
pub const Page = @import("page.zig").Page;
const Schema = @import("schema.zig");
const QueryParser = @import("query_parser.zig");

pub const DbInfo = struct {
    page_size: u16,
    table_count: u16,

    pub fn read(file: std.fs.File) !DbInfo {
        // read page size
        var page_size_buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&page_size_buf);
        const page_size = std.mem.readInt(u16, &page_size_buf, .big);

        var page_type_buf: [1]u8 = undefined;
        _ = try file.seekTo(100);
        _ = try file.read(&page_type_buf);
        const page_type = page_type_buf[0];
        std.debug.assert(page_type == 0x0D);

        var table_count_buf: [2]u8 = undefined;
        _ = try file.seekBy(2);
        _ = try file.read(&table_count_buf);
        const table_count = std.mem.readInt(u16, &table_count_buf, .big);

        return .{
            .page_size = page_size,
            .table_count = table_count,
        };
    }
};

pub const Database = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    page_size: usize,

    pub fn init(allocator: std.mem.Allocator, file: std.fs.File) !Database {
        const info = try DbInfo.read(file);
        return Database{
            .allocator = allocator,
            .file = file,
            .page_size = info.page_size,
        };
    }

    pub fn readPage(self: *Database, page_number: usize) !Page {
        const offset = if (page_number == 1) 100 else (page_number - 1) * self.page_size;
        try self.file.seekTo(offset);
        return Page.read(self.allocator, self.file.reader());
    }

    fn compareValues(value: Page.Cell.Record.Value, condition: QueryParser.Condition) bool {
        if (!std.mem.eql(u8, condition.operator, "=")) return false;

        switch (value) {
            .Text => |text| return std.mem.eql(u8, text, condition.value),
            .Integer => |int| {
                const parsed = std.fmt.parseInt(i64, condition.value, 10) catch return false;
                return int == parsed;
            },
            .Null => return false,
        }
    }

    fn printColumnValue(writer: anytype, value: Page.Cell.Record.Value) !void {
        switch (value) {
            .Text => |text| try writer.print("{s}", .{text}),
            .Integer => |int| try writer.print("{d}", .{int}),
            .Null => try writer.print("NULL", .{}),
        }
    }

    pub fn executeQuery(self: *Database, sql: []const u8) !void {
        const query = try QueryParser.parseQuery(self.allocator, sql);
        defer switch (query) {
            .count => |c| self.allocator.free(c.table_name),
            .select => |s| {
                self.allocator.free(s.table_name);
                for (s.columns) |col| self.allocator.free(col);
                self.allocator.free(s.columns);
                if (s.condition) |cond| {
                    self.allocator.free(cond.column);
                    self.allocator.free(cond.operator);
                    self.allocator.free(cond.value);
                }
            },
        };

        // read schema page (always page 1)
        var schema_page = try self.readPage(1);
        defer schema_page.deinit();

        const stdout = std.io.getStdOut().writer();

        switch (query) {
            .count => |count_query| {
                const table_schema = Schema.findTable(schema_page, count_query.table_name) orelse
                    return error.TableNotFound;

                var table_page = try self.readPage(table_schema.root_page);
                defer table_page.deinit();

                try stdout.print("{d}\n", .{table_page.cells.len});
            },
            .select => |select_query| {
                const table_schema = Schema.findTable(schema_page, select_query.table_name) orelse
                    return error.TableNotFound;

                // get indices for all columns
                var column_indices = try self.allocator.alloc(usize, select_query.columns.len);
                defer self.allocator.free(column_indices);

                for (select_query.columns, 0..) |col, i| {
                    column_indices[i] = try table_schema.findColumnIndex(col);
                }

                // get where condition column index if needed
                const where_col_idx = if (select_query.condition) |cond|
                    try table_schema.findColumnIndex(cond.column)
                else
                    null;

                var table_page = try self.readPage(table_schema.root_page);
                defer table_page.deinit();

                // print each matching row
                for (table_page.cells) |cell| {
                    // check WHERE condition if present
                    if (select_query.condition) |cond| {
                        if (where_col_idx) |idx| {
                            if (idx >= cell.payload.values.len or
                                !compareValues(cell.payload.values[idx], cond))
                            {
                                continue;
                            }
                        }
                    }

                    // print matching row
                    for (column_indices, 0..) |col_idx, i| {
                        if (i > 0) try stdout.print("|", .{});
                        if (col_idx >= cell.payload.values.len) continue;
                        try printColumnValue(stdout, cell.payload.values[col_idx]);
                    }
                    try stdout.print("\n", .{});
                }
            },
        }
    }
};
