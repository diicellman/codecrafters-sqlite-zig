const std = @import("std");

pub const Query = union(enum) {
    count: struct {
        table_name: []const u8,
    },
    select: struct {
        column_name: []const u8,
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
    const column_name = what;

    const from = it.next() orelse return error.InvalidQuery;
    if (!std.ascii.eqlIgnoreCase(from, "FROM")) {
        return error.InvalidQuery;
    }

    const table_name = it.next() orelse return error.InvalidQuery;

    const owned_table_name = try allocator.dupe(u8, table_name);
    const owned_column_name = try allocator.dupe(u8, column_name);

    return Query{ .select = .{
        .column_name = owned_column_name,
        .table_name = owned_table_name,
    } };
}
