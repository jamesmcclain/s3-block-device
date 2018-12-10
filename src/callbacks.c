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
    } else if (strcmp(path, device_name) == 0) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = device_size;
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

    fi->direct_io = 0;
    return 0;
}

int s3bd_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

int s3bd_read(const char *path, char *buf, size_t size,
              off_t offset, struct fuse_file_info *fi)
{
    char block_path[0x1000];
    int64_t block_number = offset / block_size;
    int64_t block_offset = offset - (block_size * offset);
    int64_t bytes_wanted = MIN(block_size - block_offset, size);

    block_to_filename(block_number, block_path);

    /* If block  can be found,  return relevant part of  its contents.
       If  it cannot  be found,  return all  zeros (that  part of  the
       virtual block device has not been written to, yet). */
    if (access(block_path, R_OK) != -1) {
        int fd = open(block_path, O_RDONLY);
        int bytes_read = read(fd, buf, bytes_wanted);
        close(fd);
        return bytes_read;
    } else {
        memset(buf, 0, bytes_wanted);
        return bytes_wanted;
    }
}

int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
    char block_path[0x1000];
    int64_t block_number = offset / block_size;
    int64_t block_offset = offset - (block_size * offset);
    int64_t bytes_wanted = MIN(block_size - block_offset, size);
    int fd, bytes_written;

    block_to_filename(block_number, block_path);

    if (access(block_path, W_OK) != -1) {       // File exists and is writable
        fd = open(block_path, O_WRONLY);
        lseek(fd, block_offset, SEEK_SET);
        bytes_written = write(fd, buf, bytes_wanted);
        close(fd);
        return bytes_written;
    } else if (access(block_path, F_OK) == -1) {        // File does not exist, make it
        fd = open(block_path, O_WRONLY);
        ftruncate(fd, block_size);
        lseek(fd, block_offset, SEEK_SET);
        bytes_written = write(fd, buf, bytes_wanted);
        close(fd);
        return bytes_written;
    } else {                    // Evidently the file exists but is not writable
        return -EDQUOT;
    }
}

int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
}
