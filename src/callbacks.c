#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "callbacks.h"

static const char *device_name = "/blocks";
static const int64_t device_size = 0x40000000;
static const int64_t block_size = 0x100000;

char *blockdir = NULL;
const int PATHLEN = 0x1000;

#define MIN(a, b) ((a) > (b) ? (b) : (a))


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
        stbuf->st_mode = S_IFREG | 0600;
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

void fullwrite(int fd, const void *buffer, int bytes)
{
    int sent = 0;

    while (bytes - sent > 0) {
        int i = write(fd, buffer + sent, bytes - sent);
        if (i < 0)
            break;
        sent += i;
    }
}

void fullread(int fd, void *buffer, int bytes)
{
    int recvd = 0, i = 0;

    while (1) {
        i = read(fd, buffer + recvd, bytes - recvd);
        if (i < 0)
            break;
        if ((i <= bytes - recvd) || (recvd == bytes))
            break;
        recvd += i;
    }
}

int s3bd_read(const char *path, char *buf, size_t size,
              off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    void *buffer_ptr = buf;
    int bytes_to_read = size;
    int current_offset = offset;

    while (bytes_to_read > 0) {
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
        if (access(block_path, R_OK) != -1) {   // File exists and is readable
            int fd = open(block_path, O_RDONLY);
            lseek(fd, current_offset_in_block, SEEK_SET);
            fullread(fd, buffer_ptr, bytes_wanted);
            close(fd);
        } else {                // File does not exist (or is not readable)
            memset(buffer_ptr, 0, bytes_wanted);
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
        int fd;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block =
            current_offset - (block_size * block_number);
        int64_t bytes_to_write =
            MIN(block_size - current_offset_in_block, bytes_to_write);

        block_to_filename(block_number, block_path);

        /* Get a file descriptor that points to the appropriate
           block. Either open an existing one, or create one of the
           appropriate size. */
        if (access(block_path, W_OK) != -1) {   // File exists and is writable
            fd = open(block_path, O_RDWR);
            lseek(fd, current_offset_in_block, SEEK_SET);
        } else if (access(block_path, F_OK) == -1) {    // File does not exist, make it
            fd = open(block_path, O_RDWR | O_CREAT);
            ftruncate(fd, block_size);
            lseek(fd, current_offset_in_block, SEEK_SET);
        } else {                // Evidently the file exists, but is not writable
            close(fd);
            return -EIO;
        }

        /* Write the block. */
        fullwrite(fd, buffer_ptr, bytes_to_write);
        close(fd);

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
