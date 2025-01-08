const std = @import("std");

pub const Op = enum {
    Eq, // =
    Ne, // != or <>
    Lt, //
    Le, // <=
    Gt, // >
    Ge, // >=
};

pub const Condition = struct {
    column: []const u8,
    operator: Op, // changed from []const u8 to Op
    value: []const u8,
};

pub const Query = union(enum) {
    count: struct {
        table_name: []const u8,
    },
    select: struct {
        columns: []const []const u8,
        table_name: []const u8,
        condition: ?Condition = null,
    },
};

fn parseOperator(op_str: []const u8) !Op {
    if (std.mem.eql(u8, op_str, "=")) return .Eq;
    if (std.mem.eql(u8, op_str, "!=") or std.mem.eql(u8, op_str, "<>")) return .Ne;
    if (std.mem.eql(u8, op_str, "<")) return .Lt;
    if (std.mem.eql(u8, op_str, "<=")) return .Le;
    if (std.mem.eql(u8, op_str, ">")) return .Gt;
    if (std.mem.eql(u8, op_str, ">=")) return .Ge;

    return error.InvalidOperator;
}

fn parseCondition(allocator: std.mem.Allocator, it: *std.mem.SplitIterator(u8, .sequence)) !?Condition {
    // look for WHERE keyword
    const where_word = it.next() orelse return null;
    if (!std.ascii.eqlIgnoreCase(where_word, "WHERE")) {
        return error.InvalidQuery;
    }

    const column = it.next() orelse return error.InvalidQuery;
    const op_str = it.next() orelse return error.InvalidQuery;
    const value = it.next() orelse return error.InvalidQuery;

    // remove quotes from value if present
    var clean_value = value;
    if (value.len >= 2 and value[0] == '\'' and value[value.len - 1] == '\'') {
        clean_value = value[1 .. value.len - 1];
    }

    return Condition{
        .column = try allocator.dupe(u8, column),
        .operator = try parseOperator(op_str), // parse operator string into enum
        .value = try allocator.dupe(u8, clean_value),
    };
}

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
    var found_from = false;
    while (it.next()) |word| {
        if (std.ascii.eqlIgnoreCase(word, "FROM")) {
            found_from = true;
            break;
        }

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
    }

    if (!found_from) return error.InvalidQuery;

    const table_name = it.next() orelse return error.InvalidQuery;
    const owned_table_name = try allocator.dupe(u8, table_name);

    // try to parse WHERE condition if present
    const condition = try parseCondition(allocator, &it);

    return Query{ .select = .{
        .columns = try columns.toOwnedSlice(),
        .table_name = owned_table_name,
        .condition = condition,
    } };
}
