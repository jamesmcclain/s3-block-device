#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

#include "cmdline.h"


#define S3BD_OPT(t, p, v) { t, offsetof(struct s3bd_configuration, p), v }

enum {
    KEY_HELP,
    KEY_VERSION,
};

static struct fuse_opt _s3bd_options[] = {
    S3BD_OPT("location=%s", location, 0),
    S3BD_OPT("ro", readonly, 1),
    S3BD_OPT("readonly", readonly, 1),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_KEY("-v", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_END
};

void * s3bd_options = _s3bd_options;

int s3bd_option_processor(void *data, const char *arg, int key,
                                 struct fuse_args *outargs)
{
    if (key == KEY_HELP) {
        fprintf(stderr,
                "usage: s3bd blockdir mountpoint [options]\n"
                "\n"
                "general options:\n"
                "\t-o opt,[opt...] \t mount options\n"
                "\t-h   --help     \t print help\n"
                "\t-V   --version  \t print version\n"
                "s3bd options:\n"
                "\t-o ro           \t read-only\n");
        exit(0);
    } else if (key == KEY_VERSION) {
        fprintf(stderr, "0.0.1\n");
        exit(0);
    }

    fprintf(stderr, "XXX %d %s\n", key, arg);
    return 1;
}
