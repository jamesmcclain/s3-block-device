#define FUSE_USE_VERSION (26)

#include <stdio.h>
#include <string.h>

#include <fuse.h>

#include "callbacks.h"
#include "cmdline.h"


static struct fuse_operations operations = {
    .getattr = s3bd_getattr,
    .readdir = s3bd_readdir,
    .open = s3bd_open,
    .flush = s3bd_flush,
    .read = s3bd_read,
    .write = s3bd_write,
    .fsync = s3bd_fsync
};


int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

#if defined(DEBUG)
    fuse_opt_parse(&args, NULL, NULL, NULL);
    configuration.blockdir = "/tmp/blockdir";
#else
    fuse_opt_parse(&args, &configuration, s3bd_options,
                   s3bd_option_processor);
#endif

    fprintf(stderr, "blockdir=%s mountpoint=%s ro=%d\n",
            configuration.blockdir, configuration.mountpoint,
            configuration.readonly);
    blockdir = configuration.blockdir;

    return fuse_main(args.argc, args.argv, &operations, NULL);
}
