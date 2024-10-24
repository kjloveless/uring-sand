const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;


const BLOCK_SZ: u64 = 4096;

const BLKGETSIZE64: u32 = linux.IOCTL.IOR(0x12, 114, posix.blksize_t);

// Returns the size of the file whose open file descriptor is passed in.
// Properly handles regular files and block devices as well.
fn get_file_size(fd: posix.fd_t) !posix.off_t {
    const st = try posix.fstat(fd);

    if (linux.S.ISBLK(st.mode)) {
        const bytes: usize = undefined;
        if (linux.ioctl(fd, BLKGETSIZE64, bytes) != 0) {
            return error.IOCTL;
        }
        return bytes;
    } else if (linux.S.ISREG(st.mode)) {
        return st.size;
    }

    return -1;
}

// Output a string of characters of len length to stdout.
// We use buffered output here to be efficient, since we
// need to output character-by-character.
fn output_to_console(buf: [*] u8, len: usize) !void {
    const out = std.io.getStdOut();
    var bw = std.io.bufferedWriter(out.writer());
    const w = bw.writer();
    var i: usize = 0;

    // don't think this is actually works the way it should...
    while (i < len) {
        try w.writeByte(buf[i]);
        i += 1;
    }
    try bw.flush();
}

fn read_and_print_file(file_name: []u8) !u32 {
   const file_fd: posix.fd_t = try posix.open(file_name, posix.O{}, 0);

   if (file_fd < 0) {
       return error.OPEN;
   }

   const file_sz: posix.off_t = try get_file_size(file_fd);
   //std.debug.print("\nfile size: {d}\n", .{file_sz});
   var bytes_remaining: usize = @intCast(file_sz);
   //std.debug.print("\nbytes remaining: {d}\n", .{bytes_remaining});
   var blocks: usize = @divTrunc(@as(usize, @intCast(file_sz)), BLOCK_SZ);
   if (@mod(file_sz,  BLOCK_SZ) > 0) {
       blocks += 1;
   }
    
   var iovecs: []posix.iovec = try std.heap.page_allocator.alloc(posix.iovec,
       blocks);
   var current_block: usize = 0;

   var buffer_pool_buffers = [_][BLOCK_SZ]u8{undefined} ** BLOCK_SZ;
   while (bytes_remaining > 0) {
       var bytes_to_read: usize = bytes_remaining;
       if (bytes_to_read > BLOCK_SZ) {
           bytes_to_read = BLOCK_SZ;
       }

       iovecs[current_block] = posix.iovec{
           .base = &buffer_pool_buffers[current_block],
            .len = bytes_to_read};
       current_block += 1;
       //std.debug.print("br: {d} - btr: {d}\n", .{bytes_remaining, bytes_to_read});
       bytes_remaining -= bytes_to_read;
   }

   //std.debug.print("{d}, {any}", .{file_fd, iovecs});
   const ret: usize = try posix.readv(file_fd, iovecs);
   if (ret < 0) {
       std.debug.print("readv", .{});
       return 1;
   }

   var i: usize = 0;
   while (i < blocks) {
       try output_to_console(iovecs[i].base, iovecs[i].len);
       i += 1;
   }

   return 0;
}

pub fn main() !void {
    const args: [][] u8 = try std.process.argsAlloc(std.heap.page_allocator);
    if (args.len < 2) {
        std.debug.print("Usage: {s} <filename1> [<filename2>...]\n", .{args[0]});
        return; 
    }

    var i: usize = 1;
    while (i < args.len) {
        //std.debug.print("{s}\n", .{args[i]});
        if (try read_and_print_file(args[i]) > 0) {
            std.debug.print("Error reading file\n", .{});
            return;
        }
        i += 1;
    }

    return; 
}
