const std = @import("std");

pub const Tag = enum {
    invalid,
    identifier,
    eof,
    kw_create,
    kw_table,
    kw_index,
    kw_select,
    kw_from,
    kw_where,
    kw_if,
    kw_not,
    kw_exists,
    kw_on,
    open_brackets,
    close_brackets,
    semicolon,
    comma,
    star,
    string_literal,
    equals, // =
    notequals, // !=
    lt, // <
    lte, // <=
    gt, // >
    gte, // >=
};

pub const Token = struct {
    tag: Tag,
    loc: Loc,

    const Loc = struct {
        start: usize,
        end: usize,
    };

    pub const keywords = std.StaticStringMapWithEql(
        Tag,
        std.ascii.eqlIgnoreCase,
    ).initComptime(.{
        .{ "SELECT", .kw_select },
        .{ "FROM", .kw_from },
        .{ "CREATE", .kw_create },
        .{ "TABLE", .kw_table },
        .{ "INDEX", .kw_index },
        .{ "WHERE", .kw_where },
        .{ "IF", .kw_if },
        .{ "NOT", .kw_not },
        .{ "EXISTS", .kw_exists },
        .{ "ON", .kw_on },
    });

    pub fn getKeyword(bytes: []const u8) ?Tag {
        return keywords.get(bytes);
    }
};

pub const Tokenizer = struct {
    buffer: [:0]const u8,
    index: usize,

    const State = enum {
        start,
        identifier,
        string_literal,
    };

    pub fn init(buffer: [:0]const u8) Tokenizer {
        return .{
            .buffer = buffer,
            .index = 0,
        };
    }

    pub fn next(self: *Tokenizer) Token {
        var state: State = .start;
        var result = Token{
            .tag = .eof,
            .loc = .{
                .start = self.index,
                .end = undefined,
            },
        };

        while (true) : (self.index += 1) {
            const c = self.buffer[self.index];
            switch (state) {
                .start => switch (c) {
                    'a'...'z', 'A'...'Z', '_' => {
                        state = .identifier;
                        result.tag = .identifier;
                    },
                    ' ', '\n', '\t', '"' => {
                        result.loc.start = self.index + 1;
                    },
                    0 => {
                        if (self.index != self.buffer.len) {
                            result.tag = .invalid;
                            result.loc.start = self.index;
                            self.index += 1;
                            return result;
                        }
                        break;
                    },
                    '*' => {
                        result.tag = .star;
                        self.index += 1;
                        break;
                    },
                    '(' => {
                        result.tag = .open_brackets;
                        self.index += 1;
                        break;
                    },
                    ')' => {
                        result.tag = .close_brackets;
                        self.index += 1;
                        break;
                    },
                    ',' => {
                        result.tag = .comma;
                        self.index += 1;
                        break;
                    },
                    ';' => {
                        result.tag = .semicolon;
                        self.index += 1;
                        break;
                    },
                    '\'' => {
                        result.tag = .string_literal;
                        state = .string_literal;
                        self.index += 1;
                        result.loc.start = self.index;
                    },
                    '=' => {
                        result.tag = .equals;
                        self.index += 1;
                        break;
                    },
                    '!' => {
                        if (self.index + 1 < self.buffer.len and self.buffer[self.index + 1] == '=') {
                            result.tag = .notequals;
                            self.index += 2;
                            break;
                        }
                        result.tag = .invalid;
                        self.index += 1;
                        break;
                    },
                    '<' => {
                        if (self.index + 1 < self.buffer.len) {
                            switch (self.buffer[self.index + 1]) {
                                '=' => {
                                    result.tag = .lte;
                                    self.index += 2;
                                    break;
                                },
                                '>' => {
                                    result.tag = .notequals;
                                    self.index += 2;
                                    break;
                                },
                                else => {
                                    result.tag = .lt;
                                    self.index += 1;
                                    break;
                                },
                            }
                        } else {
                            result.tag = .lt;
                            self.index += 1;
                            break;
                        }
                    },
                    '>' => {
                        if (self.index + 1 < self.buffer.len and self.buffer[self.index + 1] == '=') {
                            result.tag = .gte;
                            self.index += 2;
                            break;
                        }
                        result.tag = .gt;
                        self.index += 1;
                        break;
                    },
                    else => {
                        result.tag = .invalid;
                        self.index += 1;
                        break;
                    },
                },
                .identifier => switch (c) {
                    'a'...'z', 'A'...'Z', '_' => {},
                    else => {
                        if (Token.getKeyword(self.buffer[result.loc.start..self.index])) |tag| {
                            result.tag = tag;
                        }
                        break;
                    },
                },
                .string_literal => switch (c) {
                    '\'' => {
                        result.loc.end = self.index;
                        self.index += 1;
                        return result;
                    },
                    else => {},
                },
            }
        }

        if (result.tag == .eof) {
            result.loc.start = self.index;
        }
        result.loc.end = self.index;
        return result;
    }
};

test "run tokenizer create table" {
    const s =
        \\CREATE TABLE IF NOT EXISTS "superheroes" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text);
    ;
    var tokenizer = Tokenizer.init(s);
    var token = tokenizer.next();
    while (token.tag != .eof) : (token = tokenizer.next()) {
        std.debug.print("{any} {s}\n", .{ token, tokenizer.buffer[token.loc.start..token.loc.end] });
    }
}

test "run tokenizer select" {
    const s =
        \\select id, name from superheroes where eye_color = 'Pink Eyes'
    ;
    var tokenizer = Tokenizer.init(s);
    var token = tokenizer.next();
    while (true) : (token = tokenizer.next()) {
        std.debug.print("{any} {s}\n", .{ token, tokenizer.buffer[token.loc.start..token.loc.end] });
        if (token.tag == .eof) {
            break;
        }
    }
}
