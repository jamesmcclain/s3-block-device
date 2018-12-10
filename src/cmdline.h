#ifndef __CMDLINE_H__
#define __CMDLINE_H__

#define FUSE_USE_VERSION (26)

#include <fuse.h>


struct s3bd_configuration {
    char *blockdir;
    char *location;
    int readonly;
};

extern void * s3bd_options;

extern int s3bd_option_processor(void *data, const char *arg, int key,
                                 struct fuse_args *outargs);

#endif
