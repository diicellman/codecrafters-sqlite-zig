const std = @import("std");
const Schema = @import("schema.zig").Schema;
const SchemaRow = @import("schema.zig").SchemaRow;
const Page = @import("page.zig").Page;
const SQL = @import("sql_parser.zig");
const TableLeafCell = @import("page.zig").TableLeafCell;
const RecordPayload = @import("record.zig").RecordPayload;

fn findTable(schema: Schema, tbl_name: [:0]const u8) ?SchemaRow {
    for (schema.rows) |row| {
        switch (row.statement) {
            .createTable => |v| {
                if (std.ascii.eqlIgnoreCase(v.tbl_name, tbl_name)) {
                    return row;
                }
            },
            else => {},
        }
    }
    return null;
}

fn findIndex(schema: Schema, tbl_name: [:0]const u8) ?SchemaRow {
    for (schema.rows) |row| {
        switch (row.statement) {
            .createIndex => |v| {
                if (std.ascii.eqlIgnoreCase(v.tbl_name, tbl_name)) {
                    return row;
                }
            },
            else => {},
        }
    }
    return null;
}

fn makeColumnMap(allocator: std.mem.Allocator, statement: SQL.Statement) !std.StringHashMap(usize) {
    var map = std.StringHashMap(usize).init(allocator);

    const columns = switch (statement) {
        .createIndex => |v| v.columns,
        .createTable => |v| v.columns,
        else => return error.NotImplemented,
    };

    for (columns, 0..) |column, i| {
        try map.put(try allocator.dupe(u8, column.name), i);
    }
    return map;
}

fn getColumnIdx(columnMap: std.StringHashMap(usize), where_column: [:0]const u8) ?usize {
    var it = columnMap.iterator();
    while (it.next()) |e| {
        if (std.mem.eql(u8, e.key_ptr.*, where_column)) {
            return e.value_ptr.*;
        }
    }
    return null;
}

fn findRowIdsWithIndex(
    allocator: std.mem.Allocator,
    reader: std.fs.File.Reader,
    index: SchemaRow,
    where: SQL.Where,
    page_size: u16,
) !std.ArrayList(u64) {
    var columnMap = try makeColumnMap(allocator, index.statement);
    defer {
        var it = columnMap.iterator();
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
        }
        columnMap.deinit();
    }

    const rootpage: u32 = @intCast(index.rootpage);
    return traverseIndex(
        allocator,
        reader,
        where,
        rootpage,
        page_size,
    );
}

pub fn query(
    allocator: std.mem.Allocator,
    reader: std.fs.File.Reader,
    statement: [:0]const u8,
) !void {
    var select = switch (try SQL.Statement.init(allocator, statement)) {
        .select => |v| v,
        else => return error.Invalid,
    };

    defer select.deinit(allocator);

    try reader.context.seekTo(16);
    const page_size = try reader.readInt(u16, .big);
    var schema = try Schema.init(allocator, reader, page_size);
    defer schema.deinit(allocator);

    const tbl_name = select.tbl_name;
    const table = findTable(schema, tbl_name) orelse return error.NoSuchTable;
    const index = findIndex(schema, tbl_name);

    var columnMap = try makeColumnMap(allocator, table.statement);
    defer {
        var it = columnMap.iterator();
        while (it.next()) |e| {
            allocator.free(e.key_ptr.*);
        }
        columnMap.deinit();
    }

    const rootpage: u32 = @intCast(table.rootpage);

    if (index != null and select.where != null) {
        const rowIds = try findRowIdsWithIndex(
            allocator,
            reader,
            index.?,
            select.where.?,
            page_size,
        );
        defer rowIds.deinit();

        for (rowIds.items) |rowId| {
            try traverseCellsWithIndex(
                allocator,
                reader,
                select,
                columnMap,
                rootpage,
                page_size,
                rowId,
            );
        }
    } else {
        try traverseCells(
            allocator,
            reader,
            select,
            columnMap,
            rootpage,
            page_size,
        );
    }
}

fn traverseCellsWithIndex(
    allocator: std.mem.Allocator,
    reader: std.fs.File.Reader,
    select: SQL.Select,
    columnMap: std.StringHashMap(usize),
    rootpage: u32,
    page_size: u32,
    rowId: u64,
) !void {
    var pages_to_read = std.ArrayList(u32).init(allocator);
    defer pages_to_read.deinit();
    try pages_to_read.append(rootpage);

    while (pages_to_read.items.len > 0) {
        const page_num = pages_to_read.orderedRemove(0);
        try reader.context.seekTo(page_size * (page_num - 1));
        var page = try Page.init(allocator, reader, page_size);
        defer page.deinit(allocator);

        switch (page.cells) {
            .tbl_leaf => |cells| try printTableLeafCells(cells, select, columnMap),
            .tbl_interior => |cells| {
                var left_key: ?u64 = null;
                var right_key: ?u64 = null;
                try pages_to_read.append(page.right_pointer.?);

                for (cells) |cell| {
                    if (rowId > cell.key) {
                        if (left_key == null or cell.key >= left_key.?) {
                            left_key = cell.key;
                        }
                    } else if (rowId <= cell.key) {
                        if (right_key == null or cell.key <= right_key.?) {
                            right_key = cell.key;
                        }
                    }
                }

                if (left_key == null and right_key == null) {
                    continue;
                }

                for (cells) |cell| {
                    if (left_key != null and cell.key < left_key.?) {
                        continue;
                    }
                    if (right_key != null and cell.key > right_key.?) {
                        continue;
                    }
                    try pages_to_read.insert(0, cell.left_page_num);
                }
            },
            else => return error.NotImplemented,
        }
    }
}

fn traverseCells(
    allocator: std.mem.Allocator,
    reader: std.fs.File.Reader,
    select: SQL.Select,
    columnMap: std.StringHashMap(usize),
    rootpage: u32,
    page_size: u32,
) !void {
    var pages_to_read = std.ArrayList(u32).init(allocator);
    defer pages_to_read.deinit();
    try pages_to_read.append(rootpage);

    while (pages_to_read.items.len > 0) {
        const page_num = pages_to_read.orderedRemove(0);
        try reader.context.seekTo(page_size * (page_num - 1));
        var page = try Page.init(allocator, reader, page_size);
        defer page.deinit(allocator);

        switch (page.cells) {
            .tbl_leaf => |cells| try printTableLeafCells(cells, select, columnMap),
            .tbl_interior => |cells| {
                for (cells) |cell| {
                    try pages_to_read.insert(0, cell.left_page_num);
                }
            },
            else => return error.NotImplemented,
        }
    }
}

fn traverseIndex(
    allocator: std.mem.Allocator,
    reader: std.fs.File.Reader,
    where: SQL.Where,
    rootpage: u32,
    page_size: u32,
) !std.ArrayList(u64) {
    var pages_to_read = std.ArrayList(u32).init(allocator);
    defer pages_to_read.deinit();
    try pages_to_read.append(rootpage);

    var rowIds = std.ArrayList(u64).init(allocator);

    while (pages_to_read.items.len > 0) {
        const page_num = pages_to_read.orderedRemove(0);
        try reader.context.seekTo(page_size * (page_num - 1));
        var page = try Page.init(allocator, reader, page_size);
        defer page.deinit(allocator);

        switch (page.cells) {
            .idx_leaf => |cells| {
                for (cells) |cell| {
                    const text = cell.payload.payloads.items[0].Text;
                    const rowid: u64 = @intCast(cell.payload.payloads.items[1].Int);
                    if (std.mem.eql(u8, text, where.cond)) {
                        try rowIds.append(rowid);
                    }
                }
            },
            .idx_interior => |cells| {
                var left_key = cells[0].payload.payloads.items[0];
                var right_key = cells[cells.len - 1].payload.payloads.items[0];

                for (cells) |cell| {
                    const payload = cell.payload.payloads.items[0];
                    switch (payload.compare(RecordPayload{ .Text = where.cond })) {
                        .lt => {
                            if (payload.compare(left_key) == .gt) {
                                left_key = payload;
                            }
                        },
                        .gt => {
                            if (payload.compare(right_key) == .lt) {
                                right_key = payload;
                            }
                        },
                        .eq => continue,
                    }
                }

                for (cells) |cell| {
                    const payload = cell.payload.payloads.items[0];
                    const rowid: u64 = @intCast(cell.payload.payloads.items[1].Int);

                    if (payload.compare(RecordPayload{ .Text = where.cond }) == .eq) {
                        try rowIds.append(rowid);
                    }
                    if (payload.compare(left_key) == .lt) {
                        continue;
                    }
                    if (payload.compare(right_key) == .gt) {
                        continue;
                    }
                    try pages_to_read.insert(0, cell.left_page_num);
                }
            },
            else => return error.NotImplemented,
        }
    }
    return rowIds;
}

fn printTableLeafCells(cells: []const TableLeafCell, select: SQL.Select, columnMap: std.StringHashMap(usize)) !void {
    const stdout = std.io.getStdOut().writer();
    outer: for (cells) |cell| {
        if (select.where != null) {
            var it = columnMap.iterator();
            while (it.next()) |e| {
                const column = e.key_ptr.*;
                const index = e.value_ptr.*;
                const payload = cell.payload.payloads.items[index];
                if (std.mem.eql(u8, select.where.?.column, column)) {
                    switch (payload) {
                        .Text => |v| {
                            const should_include = switch (select.where.?.operator) {
                                .Eq => std.mem.eql(u8, v, select.where.?.cond),
                                .Ne => !std.mem.eql(u8, v, select.where.?.cond),
                                .Lt => std.mem.lessThan(u8, v, select.where.?.cond),
                                .Le => std.mem.lessThan(u8, v, select.where.?.cond) or std.mem.eql(u8, v, select.where.?.cond),
                                .Gt => std.mem.lessThan(u8, select.where.?.cond, v),
                                .Ge => std.mem.lessThan(u8, select.where.?.cond, v) or std.mem.eql(u8, v, select.where.?.cond),
                            };
                            if (!should_include) {
                                continue :outer;
                            }
                        },
                        .Int => |i| {
                            const cond_int = std.fmt.parseInt(isize, select.where.?.cond, 10) catch continue :outer;
                            const should_include = switch (select.where.?.operator) {
                                .Eq => i == cond_int,
                                .Ne => i != cond_int,
                                .Lt => i < cond_int,
                                .Le => i <= cond_int,
                                .Gt => i > cond_int,
                                .Ge => i >= cond_int,
                            };
                            if (!should_include) {
                                continue :outer;
                            }
                        },
                        .Null => continue :outer,
                        else => continue,
                    }
                }
            }
        }

        for (select.columns, 0..) |column, i| {
            if (i > 0) {
                try stdout.print("|", .{});
            }
            if (std.ascii.eqlIgnoreCase(column, "COUNT")) {
                const num_rows = cells.len;
                try stdout.print("{d}\n", .{num_rows});
                return;
            } else if (std.ascii.eqlIgnoreCase(column, "id")) {
                const rowid = cell.key;
                try stdout.print("{d}", .{rowid});
                continue;
            }
            const index = columnMap.get(column) orelse return error.NoSuchColumn;
            const payload = cell.payload.payloads.items[index];
            switch (payload) {
                .Text => |v| try stdout.print("{s}", .{v}),
                .Int => |v| try stdout.print("{d}", .{v}),
                else => {},
            }
        }
        try stdout.print("\n", .{});
    }
}

test "query table" {
    var file = try std.fs.cwd().openFile("sample.db", .{});
    defer file.close();
    const allocator = std.testing.allocator;
    const reader = file.reader();
    try query(allocator, reader, "select name, color from apples WHERE color = 'Light Green'");
}
