#ifndef __CALLBACKS_H__
#define __CALLBACKS_H__

#define FUSE_USE_VERSION (26)

#include <fuse.h>

extern int s3bd_getattr(const char *path, struct stat *stbuf);
extern int s3bd_readdir(const char *path, void *buf,
                        fuse_fill_dir_t filler, off_t offset,
                        struct fuse_file_info *fi);
extern int s3bd_open(const char *path, struct fuse_file_info *fi);
extern int s3bd_flush(const char *path, struct fuse_file_info *fi);
extern int s3bd_read(const char *path, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi);
extern int s3bd_write(const char *path, const char *buf, size_t size,
                      off_t offset, struct fuse_file_info *fi);
extern int s3bd_fsync(const char *path, int isdatasync,
                      struct fuse_file_info *fi);
extern int s3bd_getxattr(const char *path, const char *name, char *value,
                         size_t size);
extern int s3bd_setxattr(const char *path, const char *name,
                         const char *value, size_t size, int flags);
extern int s3bd_chmod(const char *path, mode_t mode);
extern int s3bd_chown(const char *path, uid_t uid, gid_t gid);
extern int s3bd_truncate(const char *path, off_t offset);
extern int s3bd_ftruncate(const char *path, off_t offset,
                          struct fuse_file_info *fi);
extern int s3bd_utimens(const char *path, const struct timespec tv[2]);
extern int s3bd_statfs(const char * path, struct statvfs * buf);

extern char *blockdir;

#endif
