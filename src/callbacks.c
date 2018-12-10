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
        stbuf->st_size = 33;    //device_size;
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
    char fullpath[0x1000];

    if (strcmp(path, device_name))
        return -ENOENT;

    fi->direct_io = 0;
    fi->fh = open("/tmp/blocks", O_RDWR | O_CREAT);
    return 0;
}

int s3bd_flush(const char *path, struct fuse_file_info *fi)
{
    close(fi->fh);
    fi->fh = -1;
    return 0;
}

int s3bd_read(const char *path, char *buf, size_t size,
              off_t offset, struct fuse_file_info *fi)
{
    return 33;
}

int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
}

int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
}
