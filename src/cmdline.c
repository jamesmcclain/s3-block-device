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

static const char *help_string =
    "usage: s3bd blockdir mountpoint [options]\n" "\n" "general options:\n"
    "\t-o opt,[opt...] \t mount options\n"
    "\t-h   --help     \t print help\n"
    "\t-V   --version  \t print version\n"
    "s3bd options:\n"
    "\t-o ro           \t read-only\n";


int s3bd_option_processor(void *data, const char *arg, int key,
                          struct fuse_args *outargs)
{
    struct s3bd_configuration *conf = (struct s3bd_configuration *) data;

    if (key == FUSE_OPT_KEY_OPT) {
        fprintf(stderr, "Unknown option or flag %s\n", arg);
        fprintf(stderr, "%s", help_string);
#if defined(DEBUG)
        return 0;
#else
        exit(-1);
#endif
    } else if (key == KEY_HELP) {
        fprintf(stderr, "%s", help_string);
#if !defined(DEBUG)
        exit(0);
#endif
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

}
