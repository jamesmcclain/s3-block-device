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
    .fsync = s3bd_fsync,
    .getxattr = s3bd_getxattr,
    .setxattr = s3bd_setxattr,
    .chmod = s3bd_chmod,
    .chown = s3bd_chown,
    .truncate = s3bd_truncate,
    .ftruncate = s3bd_ftruncate,
    .utimens = s3bd_utimens,
    .statfs = s3bd_statfs,
};

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    char fsname[0x1000];

    fuse_opt_parse(&args, &configuration, s3bd_options,
                   s3bd_option_processor);

    fprintf(stderr, "blockdir=%s mountpoint=%s ro=%d\n",
            configuration.blockdir, configuration.mountpoint,
            configuration.readonly);
    blockdir = configuration.blockdir;
    readonly = configuration.readonly;
    if (readonly) {
        operations.write = NULL;
    }

    sprintf(fsname, "-ofsname=%s", blockdir);
    fuse_opt_add_arg(&args, fsname);
    fuse_opt_add_arg(&args, "-oallow_other");
    return fuse_main(args.argc, args.argv, &operations, NULL);
}
