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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dlfcn.h>

#include <fuse.h>

#include "cmdline.h"

/* Backend interface */
static struct fuse_operations operations = {};

static char **blockdir = NULL;
static int64_t *block_size = NULL;
static int64_t *device_size = NULL;
static int *readonly = NULL;

int main(int argc, char **argv)
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    void *handle;
    char fsname[0x1000];

    /* Parse the command line */
    fuse_opt_parse(&args, &configuration, s3bd_options, s3bd_option_processor);

    /* Report results of parsing */
    fprintf(stderr, "backend=%s blockdir=%s mountpoint=%s ro=%d\n",
            configuration.backend, configuration.blockdir,
            configuration.mountpoint, configuration.readonly);

    if (configuration.backend == NULL || configuration.blockdir == NULL || configuration.mountpoint == NULL)
    {
        exit(EXIT_FAILURE);
    }

    /* Load the backend */
    handle = dlopen(configuration.backend, RTLD_NOW | RTLD_LOCAL);
    if (handle == NULL)
    {
        fprintf(stderr, "Unable to load backend library.\n");
        exit(EXIT_FAILURE);
    }

    /* Register callbacks */
    operations.getattr = dlsym(handle, "s3bd_getattr");
    operations.readdir = dlsym(handle, "s3bd_readdir");
    operations.open = dlsym(handle, "s3bd_open");
    operations.flush = dlsym(handle, "s3bd_flush");
    operations.read = dlsym(handle, "s3bd_read");
    if (!configuration.readonly)
    {
        operations.write = dlsym(handle, "s3bd_write");
    }
    operations.fsync = dlsym(handle, "s3bd_fsync");
    operations.getxattr = dlsym(handle, "s3bd_getxattr");
    operations.setxattr = dlsym(handle, "s3bd_setxattr");
    operations.chmod = dlsym(handle, "s3bd_chmod");
    operations.chown = dlsym(handle, "s3bd_chown");
    operations.truncate = dlsym(handle, "s3bd_truncate");
    operations.ftruncate = dlsym(handle, "s3bd_ftruncate");
    operations.utimens = dlsym(handle, "s3bd_utimens");
    operations.statfs = dlsym(handle, "s3bd_statfs");

    /* Bind variables in backend library */
    blockdir = dlsym(handle, "blockdir");
    readonly = dlsym(handle, "readonly");
    device_size = dlsym(handle, "device_size");
    block_size = dlsym(handle, "block_size");

    /* Report information from command line to backend */
    *blockdir = configuration.blockdir;
    *readonly = configuration.readonly;
    *device_size = 0x4000000; // XXX
    *block_size = sysconf(_SC_PAGESIZE);

    /* Arguments for libfuse */
    sprintf(fsname, "-ofsname=%s", *blockdir);
    fuse_opt_add_arg(&args, fsname);
    fuse_opt_add_arg(&args, "-oallow_other");

    /* Delegate to libfuse */
    return fuse_main(args.argc, args.argv, &operations, NULL);
}
