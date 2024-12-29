const std = @import("std");
pub const Page = @import("page.zig").Page;

pub const DbInfo = struct {
    page_size: u16,
    table_count: u16,

    pub fn read(file: std.fs.File) !DbInfo {
        // Read page size
        var page_size_buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&page_size_buf);
        const page_size = std.mem.readInt(u16, &page_size_buf, .big);

        var page_type_buf: [1]u8 = undefined;
        _ = try file.seekTo(100);
        _ = try file.read(&page_type_buf);
        const page_type = page_type_buf[0];
        std.debug.assert(page_type == 0x0D);

        var table_count_buf: [2]u8 = undefined;
        _ = try file.seekBy(2);
        _ = try file.read(&table_count_buf);
        const table_count = std.mem.readInt(u16, &table_count_buf, .big);

        return .{
            .page_size = page_size,
            .table_count = table_count,
        };
    }
};
