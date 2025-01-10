const std = @import("std");
const Record = @import("record.zig").Record;
const Page = @import("page.zig").Page;
const Schema = @import("schema.zig").Schema;
const SQL = @import("sql_parser.zig");
const Table = @import("table.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        return;
    }

    const database_file_path: []const u8 = args[1];
    const command: [:0]const u8 = args[2];

    const stdout = std.io.getStdOut().writer();
    var file = try std.fs.cwd().openFile(database_file_path, .{});
    defer file.close();

    if (std.mem.eql(u8, command, ".dbinfo")) {
        var buf: [2]u8 = undefined;
        try file.seekTo(16);
        _ = try file.read(&buf);
        const page_size = std.mem.readInt(u16, &buf, .big);
        try stdout.print("database page size: {}\n", .{page_size});

        try file.seekTo(103);
        _ = try file.read(&buf);
        const num_tables = std.mem.readInt(u16, &buf, .big);
        try stdout.print("number of tables: {}\n", .{num_tables});
    } else if (std.mem.eql(u8, command, ".tables")) {
        const reader = file.reader();
        try reader.context.seekTo(16);
        const page_size = try reader.readInt(u16, .big);
        try reader.context.seekTo(0);
        var page = try Page.init(allocator, reader, page_size);
        defer page.deinit(allocator);

        for (page.cells.tbl_leaf) |cell| {
            const tbl_name_payload = cell.payload.payloads.items[2];
            switch (tbl_name_payload) {
                .Text => |v| {
                    if (!std.mem.eql(u8, v, "sqlite_sequence")) {
                        try stdout.print("{s} ", .{v});
                    }
                },
                else => {},
            }
        }
        try stdout.print("\n", .{});
    } else {
        const reader = file.reader();
        try Table.query(allocator, reader, command);
    }
}
