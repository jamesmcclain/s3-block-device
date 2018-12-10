#ifndef __CALLBACKS_H__
#define __CALLBACKS_H__

#define FUSE_USE_VERSION (26)

#include <fuse.h>

extern int s3bd_getattr(const char *path, struct stat *stbuf);
extern int s3bd_open(const char *path, struct fuse_file_info *fi);
extern int s3bd_readdir(const char *path, void *buf,
                        fuse_fill_dir_t filler, off_t offset,
                        struct fuse_file_info *fi);
extern int s3bd_read(const char *path, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi);

#endif
