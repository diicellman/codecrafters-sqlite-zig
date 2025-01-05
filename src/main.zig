const std = @import("std");
const sqlite = @import("sqlite.zig");
const Database = sqlite.Database;

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

    var file = try std.fs.cwd().openFile(database_file_path, .{});
    defer file.close();

    if (std.mem.eql(u8, command, ".dbinfo")) {
        const info = try sqlite.DbInfo.read(file);
        const stdout = std.io.getStdOut().writer();
        try stdout.print("database page size: {}\n", .{info.page_size});
        try stdout.print("number of tables: {}\n", .{info.table_count});
        return;
    }

    if (std.mem.eql(u8, command, ".tables")) {
        var db = try Database.init(allocator, file);
        var schema_page = try db.readPage(1);
        defer schema_page.deinit();

        for (schema_page.cells) |cell| {
            if (cell.payload.values[0] == .Text and std.mem.eql(u8, cell.payload.values[0].Text, "table")) {
                try std.io.getStdOut().writer().print("{s} ", .{cell.payload.values[2].Text});
            }
        }
        try std.io.getStdOut().writer().print("\n", .{});
        return;
    }

    // Handle SQL queries
    var db = try Database.init(allocator, file);
    try db.executeQuery(command);
}
