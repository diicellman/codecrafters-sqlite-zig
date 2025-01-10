const std = @import("std");
pub const Token = @import("token.zig");
const Index = usize;

pub const Parser = struct {
    source: []const u8,
    tokens: []const Token.Token,
    tok_idx: Index,

    pub fn init(allocator: std.mem.Allocator, buffer: [:0]const u8) !Parser {
        var tokenizer = Token.Tokenizer.init(buffer);
        var tokens = std.ArrayList(Token.Token).init(allocator);
        errdefer tokens.deinit();

        var token = tokenizer.next();
        while (true) : (token = tokenizer.next()) {
            try tokens.append(token);
            if (token.tag == .eof) {
                break;
            }
        }

        return .{
            .source = try allocator.dupe(u8, buffer),
            .tokens = try tokens.toOwnedSlice(),
            .tok_idx = 0,
        };
    }

    pub fn deinit(self: *Parser, allocator: std.mem.Allocator) void {
        allocator.free(self.tokens);
        allocator.free(self.source);
    }

    pub fn advance(self: *Parser) Index {
        const r = self.tok_idx;
        self.tok_idx += 1;
        return r;
    }

    pub fn getToken(self: *Parser, tag: Token.Tag) ?Index {
        if (self.tokens[self.tok_idx].tag == tag) {
            return self.advance();
        }
        return null;
    }

    pub fn expectToken(self: *Parser, tag: Token.Tag) !Index {
        if (self.tokens[self.tok_idx].tag == tag) {
            return self.advance();
        }
        return error.ParseError;
    }

    pub fn getTag(self: Parser) Token.Tag {
        return self.tokens[self.tok_idx].tag;
    }

    pub fn getTokenValue(self: Parser, idx: usize) []const u8 {
        const token = self.tokens[idx];
        return self.source[token.loc.start..token.loc.end];
    }
};

test "run parser" {
    const s =
        \\CREATE TABLE apples
        \\(
        \\        id integer primary key autoincrement,
        \\        name text,
        \\        color text
        \\);
    ;
    const allocator = std.testing.allocator;
    var parser = try Parser.init(allocator, s);
    defer parser.deinit(allocator);
    _ = try parser.expectToken(.kw_create);
    _ = try parser.expectToken(.kw_table);
    const token = parser.tokens[parser.tok_idx];
    std.debug.print("{any} {s}\n", .{ token, parser.source[token.loc.start..token.loc.end] });
}
