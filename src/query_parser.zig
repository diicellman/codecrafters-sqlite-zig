const std = @import("std");

pub const Query = union(enum) {
    count: struct {
        table_name: []const u8,
    },
    // add more query types later
};

pub fn parseQuery(allocator: std.mem.Allocator, sql: []const u8) !Query {
    var it = std.mem.splitSequence(u8, sql, " ");

    // skip "SELECT"
    _ = it.next() orelse return error.InvalidQuery;

    // check for COUNT(*)
    const count = it.next() orelse return error.InvalidQuery;
    if (!std.mem.eql(u8, count, "COUNT(*)")) {
        return error.InvalidQuery;
    }

    // skip "FROM"
    const from = it.next() orelse return error.InvalidQuery;
    if (!std.mem.eql(u8, from, "FROM")) {
        return error.InvalidQuery;
    }

    // get table name
    const table_name = it.next() orelse return error.InvalidQuery;
    const owned_table_name = try allocator.dupe(u8, table_name);

    return Query{ .count = .{ .table_name = owned_table_name } };
}
