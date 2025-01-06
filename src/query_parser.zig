const std = @import("std");

pub const Query = union(enum) {
    count: struct {
        table_name: []const u8,
    },
    select: struct {
        columns: []const []const u8,
        table_name: []const u8,
    },
};

pub fn parseQuery(allocator: std.mem.Allocator, sql: []const u8) !Query {
    var it = std.mem.splitSequence(u8, sql, " ");

    // parse SELECT
    const select_word = it.next() orelse return error.InvalidQuery;
    if (!std.ascii.eqlIgnoreCase(select_word, "SELECT")) {
        return error.InvalidQuery;
    }

    const what = it.next() orelse return error.InvalidQuery;

    // handle COUNT(*)
    if (std.ascii.eqlIgnoreCase(what, "COUNT(*)")) {
        const from = it.next() orelse return error.InvalidQuery;
        if (!std.ascii.eqlIgnoreCase(from, "FROM")) {
            return error.InvalidQuery;
        }
        const table_name = it.next() orelse return error.InvalidQuery;
        const owned_table_name = try allocator.dupe(u8, table_name);
        return Query{ .count = .{ .table_name = owned_table_name } };
    }

    // handle column selection
    var columns = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (columns.items) |col| allocator.free(col);
        columns.deinit();
    }

    // handle first column(s) - could be "name,color" or just "name"
    var col_it = std.mem.splitSequence(u8, what, ",");
    while (col_it.next()) |col| {
        const trimmed = std.mem.trim(u8, col, " ");
        if (trimmed.len > 0) {
            try columns.append(try allocator.dupe(u8, trimmed));
        }
    }

    // check for more columns
    while (it.next()) |word| {
        if (std.ascii.eqlIgnoreCase(word, "FROM")) break;

        // skip standalone comma
        if (std.mem.eql(u8, word, ",")) continue;

        // split this word on commas too
        var word_cols = std.mem.splitSequence(u8, word, ",");
        while (word_cols.next()) |col| {
            const trimmed = std.mem.trim(u8, col, " ");
            if (trimmed.len > 0) {
                try columns.append(try allocator.dupe(u8, trimmed));
            }
        }
    } else {
        return error.InvalidQuery;
    }

    const table_name = it.next() orelse return error.InvalidQuery;
    const owned_table_name = try allocator.dupe(u8, table_name);

    return Query{ .select = .{
        .columns = try columns.toOwnedSlice(),
        .table_name = owned_table_name,
    } };
}
