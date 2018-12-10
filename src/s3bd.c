#define FUSE_USE_VERSION (26)

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <string.h>

#include <fuse.h>

#include "cmdline.h"


const char *block_device_name = "/blocks";


static int s3bd_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
    } else if (strcmp(path, block_device_name) == 0) {
        stbuf->st_mode = S_IFREG | 0444;
        stbuf->st_nlink = 1;
        stbuf->st_size = 33;
    } else
        res = -ENOENT;

    return res;
}

static int s3bd_open(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

static int s3bd_readdir(const char *path, void *buf,
                        fuse_fill_dir_t filler, off_t offset,
                        struct fuse_file_info *fi)
{
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, block_device_name + 1, NULL, 0);

    return 0;
}

static int s3bd_read(const char *path, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
    return 33;
}

static struct fuse_operations operations = {
    .getattr = s3bd_getattr,
    .open = s3bd_open,
    .readdir = s3bd_readdir,
    .read = s3bd_read,
};


int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct s3bd_configuration conf;
    memset(&conf, 0, sizeof(conf));

    fuse_opt_parse(&args, &conf, s3bd_options, s3bd_option_processor);

    return fuse_main(args.argc, args.argv, &operations, NULL);
}
