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

#ifndef __CALLBACKS_H__
#define __CALLBACKS_H__

#define FUSE_USE_VERSION (26)

#include <stdint.h>

#include <fuse.h>

extern int s3bd_getattr(const char *path, struct stat *stbuf);
extern int s3bd_readdir(const char *path, void *buf,
                        fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
extern int s3bd_open(const char *path, struct fuse_file_info *fi);
extern int s3bd_flush(const char *path, struct fuse_file_info *fi);
extern int s3bd_read(const char *path, char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi);
extern int s3bd_write(const char *path, const char *buf, size_t size,
                      off_t offset, struct fuse_file_info *fi);
extern int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi);
extern int s3bd_getxattr(const char *path, const char *name, char *value, size_t size);
extern int s3bd_setxattr(const char *path, const char *name,
                         const char *value, size_t size, int flags);
extern int s3bd_chmod(const char *path, mode_t mode);
extern int s3bd_chown(const char *path, uid_t uid, gid_t gid);
extern int s3bd_truncate(const char *path, off_t offset);
extern int s3bd_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi);
extern int s3bd_utimens(const char *path, const struct timespec tv[2]);
extern int s3bd_statfs(const char *path, struct statvfs *buf);

extern int64_t device_size;
extern int64_t block_size;
extern char *blockdir;
extern int readonly;

#endif
