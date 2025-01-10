const std = @import("std");
const parse = @import("parser.zig");
const Token = @import("token.zig");

pub const ParseError = error{
    Invalid,
};

pub const Op = enum {
    Eq, // =
    Ne, // != or <>
    Lt, // <
    Le, // <=
    Gt, // >
    Ge, // >=
};

pub const Statement = union(enum) {
    select: Select,
    createTable: CreateTable,
    createIndex: CreateIndex,

    pub fn init(allocator: std.mem.Allocator, sql: [:0]const u8) !Statement {
        var parser = try parse.Parser.init(allocator, sql);
        defer parser.deinit(allocator);
        switch (parser.tokens[parser.tok_idx].tag) {
            .kw_select => return Statement{ .select = try parseSelect(allocator, &parser) },
            .kw_create => {
                _ = parser.advance();
                switch (parser.tokens[parser.tok_idx].tag) {
                    .kw_table => return Statement{ .createTable = try parseCreateTable(allocator, &parser) },
                    .kw_index => return Statement{ .createIndex = try parseCreateIndex(allocator, &parser) },
                    else => return error.Invalid,
                }
            },
            else => return error.Invalid,
        }
    }

    fn getOperator(tag: Token.Tag) !Op {
        return switch (tag) {
            .equals => .Eq,
            .notequals => .Ne,
            .lt => .Lt,
            .lte => .Le,
            .gt => .Gt,
            .gte => .Ge,
            else => error.Invalid,
        };
    }

    fn parseSelect(allocator: std.mem.Allocator, parser: *parse.Parser) !Select {
        _ = try parser.expectToken(.kw_select);
        var columns = std.ArrayList([:0]u8).init(allocator);
        errdefer {
            for (columns.items) |column| {
                allocator.free(column);
            }
            columns.deinit();
        }

        while (true) {
            if (parser.getToken(.kw_from)) |_| {
                break;
            }
            const tok_idx = try parser.expectToken(.identifier);
            const column = parser.getTokenValue(tok_idx);
            if (std.ascii.eqlIgnoreCase(column, "COUNT")) {
                _ = try parser.expectToken(.open_brackets);
                _ = try parser.expectToken(.star);
                _ = try parser.expectToken(.close_brackets);
            }
            try columns.append(try allocator.dupeZ(u8, column));
            if (parser.getToken(.comma)) |_| {
                continue;
            }
        }

        var tok_idx = try parser.expectToken(.identifier);
        const tbl_name = try allocator.dupeZ(u8, parser.getTokenValue(tok_idx));
        const columns_owned = try columns.toOwnedSlice();

        _ = parser.getToken(.kw_where) orelse return Select{
            .tbl_name = tbl_name,
            .columns = columns_owned,
            .where = null,
        };

        tok_idx = try parser.expectToken(.identifier);
        const where_column = parser.getTokenValue(tok_idx);

        const op_token = parser.tokens[parser.tok_idx].tag;
        const op = try getOperator(op_token);
        _ = parser.advance();

        tok_idx = try parser.expectToken(.string_literal);
        const where_cond = parser.getTokenValue(tok_idx);

        const where = Where{
            .column = try allocator.dupeZ(u8, where_column),
            .operator = op,
            .cond = try allocator.dupeZ(u8, where_cond),
        };

        return Select{
            .tbl_name = tbl_name,
            .columns = columns_owned,
            .where = where,
        };
    }

    fn parseCreateIndex(allocator: std.mem.Allocator, parser: *parse.Parser) !CreateIndex {
        _ = try parser.expectToken(.kw_index);
        var tok_idx = try parser.expectToken(.identifier);
        const idx_name = parser.getTokenValue(tok_idx);
        _ = try parser.expectToken(.kw_on);
        tok_idx = try parser.expectToken(.identifier);
        const tbl_name = parser.getTokenValue(tok_idx);
        _ = try parser.expectToken(.open_brackets);

        var columns = std.ArrayList(Column).init(allocator);
        errdefer columns.deinit();

        var is_parsing_column = true;
        while (true) {
            if (parser.getToken(.close_brackets)) |_| {
                break;
            }
            if (is_parsing_column) {
                const column_name = parser.getTokenValue(parser.tok_idx);
                const column = Column{ .name = try allocator.dupeZ(u8, column_name) };
                try columns.append(column);
                is_parsing_column = false;
            } else if (parser.getTag() == .comma) {
                is_parsing_column = true;
            }
            _ = parser.advance();
        }

        return CreateIndex{
            .columns = try columns.toOwnedSlice(),
            .tbl_name = try allocator.dupeZ(u8, tbl_name),
            .idx_name = try allocator.dupeZ(u8, idx_name),
        };
    }

    fn parseCreateTable(allocator: std.mem.Allocator, parser: *parse.Parser) !CreateTable {
        _ = try parser.expectToken(.kw_table);
        _ = parser.getToken(.kw_if);
        _ = parser.getToken(.kw_not);
        _ = parser.getToken(.kw_exists);
        const tok_idx = try parser.expectToken(.identifier);
        const tbl_name = parser.getTokenValue(tok_idx);
        _ = try parser.expectToken(.open_brackets);

        var columns = std.ArrayList(Column).init(allocator);
        errdefer columns.deinit();

        var is_parsing_column = true;
        while (true) {
            if (parser.getToken(.close_brackets)) |_| {
                break;
            }
            if (is_parsing_column) {
                const column_name = parser.getTokenValue(parser.tok_idx);
                const column = Column{ .name = try allocator.dupeZ(u8, column_name) };
                try columns.append(column);
                is_parsing_column = false;
            } else if (parser.getTag() == .comma) {
                is_parsing_column = true;
            }
            parser.tok_idx += 1;
        }

        return .{
            .columns = try columns.toOwnedSlice(),
            .tbl_name = try allocator.dupeZ(u8, tbl_name),
        };
    }

    pub fn debug(self: Statement) void {
        switch (self) {
            .select => |v| v.debug(),
            .createTable => |v| v.debug(),
            .createIndex => |v| v.debug(),
        }
    }

    pub fn deinit(self: *Statement, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .select => |*v| v.deinit(allocator),
            .createTable => |*v| v.deinit(allocator),
            .createIndex => |*v| v.deinit(allocator),
        }
    }
};

pub const Where = struct {
    column: [:0]const u8,
    operator: Op,
    cond: [:0]const u8,

    pub fn deinit(self: *Where, allocator: std.mem.Allocator) void {
        allocator.free(self.column);
        allocator.free(self.cond);
    }
};

pub const Select = struct {
    tbl_name: [:0]const u8,
    columns: [][:0]const u8,
    where: ?Where,

    pub fn deinit(self: *Select, allocator: std.mem.Allocator) void {
        allocator.free(self.tbl_name);
        for (0..self.columns.len) |i| {
            allocator.free(self.columns[i]);
        }
        allocator.free(self.columns);

        if (self.where) |*w| {
            w.deinit(allocator);
        }
    }

    pub fn debug(self: Select) void {
        std.debug.print("tbl_name: {s}, column: {any}, where: {?s} {?any} {?s}\n", .{
            self.tbl_name,
            self.columns,
            if (self.where) |w| w.column else null,
            if (self.where) |w| w.operator else null,
            if (self.where) |w| w.cond else null,
        });
    }
};

pub const Column = struct {
    name: [:0]const u8,

    pub fn deinit(self: *Column, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
    }
};

pub const CreateTable = struct {
    tbl_name: [:0]const u8,
    columns: []Column,

    pub fn deinit(self: *CreateTable, allocator: std.mem.Allocator) void {
        for (self.columns) |*column| {
            column.deinit(allocator);
        }
        allocator.free(self.columns);
        allocator.free(self.tbl_name);
    }

    pub fn debug(self: CreateTable) void {
        for (self.columns) |column| {
            std.debug.print("column_name: {s}\n", .{column.name});
        }
    }
};

pub const CreateIndex = struct {
    tbl_name: [:0]const u8,
    idx_name: [:0]const u8,
    columns: []Column,

    pub fn deinit(self: *CreateIndex, allocator: std.mem.Allocator) void {
        for (self.columns) |*column| {
            column.deinit(allocator);
        }
        allocator.free(self.columns);
        allocator.free(self.tbl_name);
        allocator.free(self.idx_name);
    }

    pub fn debug(self: CreateIndex) void {
        std.debug.print("tbl_name: {s}, idx_name: {s}\n", .{ self.tbl_name, self.idx_name });
        for (self.columns) |column| {
            std.debug.print("column_name: {s}\n", .{column.name});
        }
    }
};

test "query select" {
    const stmt =
        \\select name, color from apples where color != 'Red'
    ;
    const allocator = std.testing.allocator;
    var c = try Statement.init(allocator, stmt);
    defer c.deinit(allocator);
    c.debug();
}

test "create table" {
    const stmt =
        \\CREATE TABLE apples
        \\(
        \\        id integer primary key autoincrement,
        \\        name text,
        \\        color text
        \\);
    ;
    const allocator = std.testing.allocator;
    var c = try Statement.init(allocator, stmt);
    defer c.deinit(allocator);
    c.debug();
}

test "create index" {
    const stmt =
        \\CREATE INDEX idx_companies_country
        \\        on companies (country)
    ;
    const allocator = std.testing.allocator;
    var c = try Statement.init(allocator, stmt);
    defer c.deinit(allocator);
    c.debug();
}
