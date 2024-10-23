const PAGE_SZ = 4 * 1024;
const BUFFER_POOL_NUM_PAGES = 1000;

const std = @import("std");

const IoUringDiskMan = struct {
    file: std.fs.File,
    io_uring: std.os.linux.IoUring,
    // iovec array filled with pointers to buffers from the buffer pool.
    // These are registered with the io_uring and used for reads and writes.
    iovec_buffers: []std.posix.iovec,

    const Self = @This();

    pub fn init(file: std.fs.File, buffer_pool_buffers:
        *[BUFFER_POOL_NUM_PAGES][PAGE_SZ]u8) !Self {
        const io_uring_queue_depth = 1024;
        const io_uring_setup_flags = 0;
        var io_uring = try std.os.linux.IoUring.init(io_uring_queue_depth,
            io_uring_setup_flags);

        // register buffers for read/write
        var iovec_buffers = try std.heap.page_allocator.alloc(std.posix.iovec,
            BUFFER_POOL_NUM_PAGES);
        errdefer std.heap.page_allocator.free(iovec_buffers);
        for (buffer_pool_buffers, 0..) |*buffer, i| {
            iovec_buffers[i] = std.posix.iovec{
                .base = buffer,
                .len = PAGE_SZ,
            };
        }
        try io_uring.register_buffers(iovec_buffers);

        return Self{
            .file = file,
            .io_uring = io_uring,
            .iovec_buffers = iovec_buffers,
        };
    }

    pub fn deinit(self: *Self) void {
        self.io_uring.deinit();
        self.file.close();
    }

    pub fn read_page(self: *Self, frame_index: usize) !void {
        std.debug.print("[IoUringDiskMan] read_page: frame_index={}\n",
            .{frame_index});
        const iovec = &self.iovec_buffers[frame_index];
        const userdata = 0x0;
        const fd = self.file.handle;
        const offset = frame_index * PAGE_SZ;
        const buffer_index: u16 = @intCast(frame_index);
        _ = try self.io_uring.read_fixed(userdata, fd, iovec, offset,
            buffer_index);
        _ = try self.io_uring.submit();
    }

    pub fn write_page(self: *Self, frame_index: usize) !void {
        std.debug.print("[IoUringDiskMan] write_page: frame_index={}\n",
            .{frame_index});
        const iovec = &self.iovec_buffers[frame_index];
        const userdata = 0x0;
        const fd = self.file.handle;
        const offset = frame_index * PAGE_SZ;
        const buffer_index: u16 = @intCast(frame_index);
        _ = try self.io_uring.write_fixed(userdata, fd, iovec, offset,
            buffer_index);
        _ = try self.io_uring.submit();
    }
};

pub const IoUringBufferPoolMan = struct {
    // Disk manager is responsible for reading and writing pages to disk.
    disk_man: *IoUringDiskMan,
    // Frames is an array of PAGE_SZ bytes of memory, each representing a page.
    frames: *[BUFFER_POOL_NUM_PAGES][PAGE_SZ]u8,
    // Page table is  mapping from page id to frame index in the buffer pool
    page_id_to_frame_map: std.AutoHashMap(u32, usize),
    // Free list is a list of free frames in the buffer pool
    free_list: std.ArrayList(usize),

    const Self = @This();

    pub fn init(frames: *[BUFFER_POOL_NUM_PAGES][PAGE_SZ]u8, disk_man:
        *IoUringDiskMan) !Self {
        var page_map = std.AutoHashMap(u32,
            usize).init(std.heap.page_allocator);
        errdefer page_map.deinit();

        var free_list = std.ArrayList(usize).init(std.heap.page_allocator);
        errdefer free_list.deinit();

        // Statically allocate the memory, prevent resizing/re-allocating.
        try page_map.ensureTotalCapacity(BUFFER_POOL_NUM_PAGES);
        try free_list.ensureTotalCapacity(BUFFER_POOL_NUM_PAGES);

        // All frames are free at the beginning.
        for (frames, 0..) |_, i| {
            try free_list.append(i);
        }
        
        return Self{
            .disk_man = disk_man,
            .frames = frames,
            .page_id_to_frame_map = page_map,
            .free_list = free_list,
        };
    }

    pub fn deinit(self: *Self) void {
        self.page_id_to_frame_map.deinit();
        self.free_list.deinit();
    }

    pub fn get_page(self: *Self, page_id: u32) !*[PAGE_SZ]u8 {
        // If the page is already in the buffer pool, return it.
        if (self.page_id_to_frame_map.contains(page_id)) {
            const frame_index = self.page_id_to_frame_map.get(page_id).?;
            return &self.frames[frame_index];
        }

        // Check if there are any free pages in the buffer pool.
        if (self.free_list.items.len == 0) {
            // TODO: Evict a page from the buffer pool.
            return error.NoFreePages;
        }

        // If the page is not in the buffer pool, read it from disk.
        const frame_index = self.free_list.pop();
        try self.disk_man.read_page(frame_index);
        _ = try self.disk_man.io_uring.submit_and_wait(1);

        // Add the page to the page table.
        self.page_id_to_frame_map.put(page_id, frame_index) catch unreachable;

        // Return the page.
        return &self.frames[frame_index];
    }

    pub fn flush_page(self: *Self, page_id: u32) !void {
        if (self.page_id_to_frame_map.contains(page_id)) {
            const frame_index = self.page_id_to_frame_map.get(page_id).?;
            try self.disk_man.write_page(frame_index);
            _ = try self.disk_man.io_uring.submit_and_wait(1);
        }
    }
};

test "io_uring disk man" {
    const file = try std.fs.cwd().createFile("test.db", .{ .truncate = true,
        .read = true });
    defer file.close();

    // Is this allocation pattern right? I'm not sure, but it passes the tests.
    var buffer_pool_buffers = [_][PAGE_SZ]u8{undefined} **
        BUFFER_POOL_NUM_PAGES;
    var disk_man  = try IoUringDiskMan.init(file, &buffer_pool_buffers);

    const page_id: u32 = 0;
    // Modify the page in the buffer pool.
    const page = &buffer_pool_buffers[page_id];
    page[0] = 0x42;
    // Submit the write to the io_uring.
    try disk_man.write_page(page_id);
    _ = try disk_man.io_uring.submit_and_wait(1);

    // Read the page from the disk (modifies the backring bufffer)
    try disk_man.read_page(page_id);
    // Wait for the read to complete.
    _ = try disk_man.io_uring.submit_and_wait(1);
    // Verify that the page was read correcy
    try std.testing.expectEqualSlices(u8, &[_]u8{0x42},
        buffer_pool_buffers[page_id][0..1]);
}

test "io_uring buffer pool manager" {
    const file = try std.fs.cwd().createFile("test.db", .{ .truncate = true,
        .read = true });
    defer file.close();

    var buffer_pool_buffers = [_][PAGE_SZ]u8{undefined} **
        BUFFER_POOL_NUM_PAGES;
    var disk_man = try IoUringDiskMan.init(file, &buffer_pool_buffers);
    var buffer_pool_man = try IoUringBufferPoolMan.init(&buffer_pool_buffers,
        &disk_man);

    const page_id: u32 = 0;
    // We expect the page to be read into frame 999, since we have 1k frames
    // and use .pop() to get the next free frame.
    const expected_frame_index = 999;

    // Modify the page in the buffer pool (page=0, frame=999).
    var page = try buffer_pool_man.get_page(page_id);
    page[0] = 0x42;

    // Flush the page to disk.
    try buffer_pool_man.flush_page(page_id);
    _ = try disk_man.io_uring.submit_and_wait(1);

    // Read the page from disk (frame=999).
    try disk_man.read_page(expected_frame_index);
    // Wait for the read to complete.
    _ = try disk_man.io_uring.submit_and_wait(1);
    // Verify that the page was read correctly.
    const updated_frame_buffer = &buffer_pool_buffers[expected_frame_index];
    try std.testing.expectEqualSlices(u8, &[_]u8{0x42},
        updated_frame_buffer[0..1]);
}
