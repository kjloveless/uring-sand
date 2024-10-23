const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;


const BLOCK_SZ: u12 = 4096;

const BLKGETSIZE64: u32 = linux.ioctl.IOR(0x12, 114, posix.blksize_t);

// Returns the size of the file whose open file descriptor is passed in.
// Properly handles regular files and block devices as well.

pub fn get_file_size(fd: posix.fd_t) !posix.off_t {
    const st: std.fs.File.Stat = try posix.fstat(fd);

    if (linux.S.ISBLK(st.mode)) {
        const bytes: usize = undefined;
        if (linux.ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
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

pub fn output_to_console(buf: []const u8, len: usize) !void {
    const out = std.io.getStdOut();
    var bw = std.io.bufferedWriter(out.writer());
    const w = bw.writer();
    var i: usize = 0;

    while (i < len) {
        try w.writeByte(buf[i]);
        i += 1;
    }
    try bw.flush();
}

pub fn main() !void {
    try output_to_console("Hello, world!", 9);
}
