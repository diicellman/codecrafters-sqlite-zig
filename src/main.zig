const std = @import("std");
const sqlite = @import("sqlite.zig");
const Page = sqlite.Page;

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
    const command: []const u8 = args[2];

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    if (std.mem.eql(u8, command, ".tables")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        _ = try file.seekTo(100);
        var page = Page.read(arena.allocator(), file.reader());

        for (page.cells) |cell| {
            // try std.io.getStdOut().writer().print("Cell\n", .{});
            try std.io.getStdOut().writer().print("{s} ", .{cell.payload.values[1].Text});
        }
        defer page.deinit();
        try std.io.getStdOut().writer().print("\n", .{});
    } else if (std.mem.eql(u8, command, ".dbinfo")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        const dbInfo = try sqlite.DbInfo.read(file);
        try std.io.getStdOut().writer().print("database page size: {}\n", .{dbInfo.page_size});
        try std.io.getStdOut().writer().print("number of tables: {}\n", .{dbInfo.table_count});
    }
}
