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
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "cmdline.h"


#define S3BD_OPT(t, p, v) { t, offsetof(struct s3bd_configuration, p), v }

enum {
    KEY_HELP,
    KEY_VERSION,
};

static struct fuse_opt _s3bd_options[] = {
    S3BD_OPT("ro", readonly, 1),
    S3BD_OPT("readonly", readonly, 1),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_KEY("-v", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_END
};

void *s3bd_options = _s3bd_options;

struct s3bd_configuration configuration = { };


int s3bd_option_processor(void *data, const char *arg, int key,
                          struct fuse_args *outargs)
{
    struct s3bd_configuration *conf = (struct s3bd_configuration *) data;

    if (key == KEY_HELP) {
        fprintf(stderr,
                "usage: %s blockdir mountpoint [options]\n\n"
                "s3bd options:\n"
                "\t-o ro           \t read-only\n\n"
                "general options:\n"
                "\t-o opt,[opt...] \t mount options (see the fuse man page)\n"
                "\t-h   --help     \t print help\n"
                "\t-V   --version  \t print version\n", outargs->argv[0]);
        exit(1);
    } else if (key == KEY_VERSION) {
        fprintf(stderr, "0.0.1\n");
        exit(0);
    } else if (key == FUSE_OPT_KEY_NONOPT && configuration.blockdir == NULL) {  // blockdir
        conf->blockdir = strdup(arg);
        return 0;
    } else if (key == FUSE_OPT_KEY_NONOPT) {    // mountpoint
        conf->mountpoint = strdup(arg);
        return 1;
    }

    return 1;
}
