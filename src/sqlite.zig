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

    pub fn executeQuery(self: *Database, sql: []const u8) !void {
        const query = try QueryParser.parseQuery(self.allocator, sql);
        defer switch (query) {
            .count => |c| self.allocator.free(c.table_name),
            .select => |s| {
                self.allocator.free(s.table_name);
                self.allocator.free(s.column_name);
            },
        };

        // read schema page (always page 1)
        var schema_page = try self.readPage(1);
        defer schema_page.deinit();

        switch (query) {
            .count => |count_query| {
                const table_schema = Schema.findTable(schema_page, count_query.table_name) orelse
                    return error.TableNotFound;

                // read the table's root page
                var table_page = try self.readPage(table_schema.root_page);
                defer table_page.deinit();

                const count = table_page.cells.len;
                try std.io.getStdOut().writer().print("{d}\n", .{count});
            },
            .select => |select_query| {
                const table_schema = Schema.findTable(schema_page, select_query.table_name) orelse
                    return error.TableNotFound;

                const column_index = try table_schema.findColumnIndex(select_query.column_name);

                // read the table's root page
                var table_page = try self.readPage(table_schema.root_page);
                defer table_page.deinit();

                // print each value in the column
                for (table_page.cells) |cell| {
                    if (column_index >= cell.payload.values.len) continue;

                    switch (cell.payload.values[column_index]) {
                        .Text => |text| try std.io.getStdOut().writer().print("{s}\n", .{text}),
                        .Integer => |int| try std.io.getStdOut().writer().print("{d}\n", .{int}),
                        .Null => try std.io.getStdOut().writer().print("NULL\n", .{}),
                    }
                }
            },
        }
    }
};
