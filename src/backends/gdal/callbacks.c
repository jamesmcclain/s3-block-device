/*
 * The MIT License
 *
 * Copyright (c) 2018 James McClain
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include "../backend.h"

static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
const int PATHLEN = 0x1000;


/*
 * Convert block number to corresponding filename.
 */
static void block_to_filename(uint64_t block_number, char *block_path)
{
    sprintf(block_path, "%s/0x%012lX", blockdir, block_number);
}

int s3bd_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_ino = 1;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
    } else if (strcmp(path, device_name) == 0) {
        if (readonly) {
            stbuf->st_mode = S_IFREG | 0400;
        } else {
            stbuf->st_mode = S_IFREG | 0600;
        }
        stbuf->st_nlink = 1;
        stbuf->st_size = device_size;
        stbuf->st_ino = 2;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_blksize = block_size;
        stbuf->st_blocks = device_size / block_size;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
    } else
        res = -ENOENT;

    return res;
}

int s3bd_readdir(const char *path, void *buf,
                 fuse_fill_dir_t filler, off_t offset,
                 struct fuse_file_info *fi)
{
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, device_name + 1, NULL, 0);

    return 0;
}

int s3bd_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, device_name))
        return -ENOENT;

    return 0;
}

int s3bd_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

int s3bd_read(const char *path, char *buf, size_t size,
              off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    void *buffer_ptr = buf;
    int bytes_to_read = size;
    int current_offset = offset;

    while (bytes_to_read > 0) {
        VSILFILE *handle = NULL;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block =
            current_offset - (block_size * block_number);
        int64_t bytes_wanted =
            MIN(block_size - current_offset_in_block, bytes_to_read);

        block_to_filename(block_number, block_path);

        /* If block can be found, return the relevant part of its
           contents.  If it cannot be found, return all zeros (that
           part of the virtual block device has not been written to,
           yet). */
        if ((handle = VSIFOpenL(block_path, "r")) == NULL) {    // Resource does not exist (or is not readable)
            memset(buffer_ptr, 0, bytes_wanted);
        } else {
            if (VSIFSeekL(handle, current_offset_in_block, SEEK_SET) == -1) {   // Resource must be seekable
                return -EIO;
            } else if (VSIFReadL(buffer_ptr, bytes_wanted, 1, handle) != 1) {   // Resource must be readable
                return -EIO;
            } else if (VSIFCloseL(handle) == -1) {      // Resource must be successfully closed
                return -EIO;
            }
        }

        /* State */
        buffer_ptr += bytes_wanted;
        current_offset += bytes_wanted;
        bytes_to_read -= bytes_wanted;
    }

    return size;
}


int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    const void *buffer_ptr = buf;
    int bytes_to_write = size;
    int current_offset = offset;

    while (bytes_to_write > 0) {
        VSILFILE *handle = NULL;
        struct stat statbuf;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block =
            current_offset - (block_size * block_number);
        volatile int64_t bytes_wanted = // Compiler bug?
            MIN(block_size - current_offset_in_block, bytes_to_write);

        block_to_filename(block_number, block_path);

        /* Get a file descriptor that points to the appropriate
           block. Either open an existing one, or create one of the
           appropriate size. */
        if (VSIStatL(block_path, (void *) &statbuf)) {  // Resource exists
            if ((handle = VSIFOpenL(block_path, "w")) == NULL) {        // Resource must be openable for write
                return -EIO;
            } else if (VSIFSeekL(handle, current_offset_in_block, SEEK_SET) == -1) {    // Resource must be seekable
                return -EIO;
            }
        } else {                // Resource does not exist, make it
            if ((handle = VSIFOpenL(block_path, "w")) == NULL) {        // Resource must be openable for write
                return -EIO;
            } else if (VSIFTruncateL(handle, block_size) != 0) {        // Resource must be truncatable
                return -EIO;
            } else if (VSIFSeekL(handle, current_offset_in_block, SEEK_SET) == -1) {    // Resource must be seekable
                return -EIO;
            }
        }

        /* Write the block. */
        if (VSIFWriteL(buffer_ptr, bytes_to_write, 1, handle) != 1) {
            return -EIO;
        } else if (VSIFCloseL(handle) == -1) {
            return -EIO;
        }

        buffer_ptr += bytes_wanted;
        current_offset += bytes_wanted;
        bytes_to_write -= bytes_wanted;
    }

    return size;
}


int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    return 0;
}

int s3bd_getxattr(const char *path, const char *name, char *value,
                  size_t size)
{
    return -ENOTSUP;
}

int s3bd_setxattr(const char *path, const char *name, const char *value,
                  size_t size, int flags)
{
    return -ENOTSUP;
}

int s3bd_chmod(const char *path, mode_t mode)
{
    return -EPERM;
}

int s3bd_chown(const char *path, uid_t uid, gid_t gid)
{
    return -EPERM;
}

int s3bd_truncate(const char *path, off_t offset)
{
    return -EPERM;
}

int s3bd_ftruncate(const char *path, off_t offset,
                   struct fuse_file_info *fi)
{
    return -EPERM;
}

int s3bd_utimens(const char *path, const struct timespec tv[2])
{
    return -EPERM;
}

int s3bd_statfs(const char *path, struct statvfs *buf)
{
    buf->f_bsize = block_size;
    buf->f_frsize = block_size;
    buf->f_blocks = device_size / block_size;
    buf->f_bfree = 0;
    buf->f_bavail = 0;
    buf->f_files = 2;
    buf->f_ffree = 0;
    buf->f_favail = 0;
    return 0;
}
