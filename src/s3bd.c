#define FUSE_USE_VERSION (26)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fuse.h>

#include "callbacks.h"
#include "cmdline.h"


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
