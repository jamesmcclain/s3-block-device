#ifndef __COMMON_H__
#define __COMMON_H__


int s3bd_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        stbuf->st_ino = 1;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
    } else if (strcmp(path, device_name) == 0) {
        if (readonly) {
            stbuf->st_mode = S_IFREG | 0400;
        } else {
            stbuf->st_mode = S_IFREG | 0600;
        }
        stbuf->st_nlink = 1;
        stbuf->st_size = device_size;
        stbuf->st_ino = 2;
        stbuf->st_uid = getuid();
        stbuf->st_gid = getgid();
        stbuf->st_blksize = block_size;
        stbuf->st_blocks = device_size / block_size;
        stbuf->st_atime = 0;
        stbuf->st_mtime = 0;
        stbuf->st_ctime = 0;
    } else
        res = -ENOENT;

    return res;
}

int s3bd_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                 off_t offset, struct fuse_file_info *fi)
{
    if (strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    filler(buf, device_name + 1, NULL, 0);

    return 0;
}

int s3bd_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, device_name))
        return -ENOENT;

    return 0;
}

int s3bd_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}


int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    return 0;
}

int s3bd_getxattr(const char *path, const char *name, char *value, size_t size)
{
    return -ENOTSUP;
}

int s3bd_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    return -ENOTSUP;
}

int s3bd_chmod(const char *path, mode_t mode)
{
    return -EPERM;
}

int s3bd_chown(const char *path, uid_t uid, gid_t gid)
{
    return -EPERM;
}

int s3bd_truncate(const char *path, off_t offset)
{
    return -EPERM;
}

int s3bd_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    return -EPERM;
}

int s3bd_utimens(const char *path, const struct timespec tv[2])
{
    return -EPERM;
}

int s3bd_statfs(const char *path, struct statvfs *buf)
{
    buf->f_bsize = block_size;
    buf->f_frsize = block_size;
    buf->f_blocks = device_size / block_size;
    buf->f_bfree = 0;
    buf->f_bavail = 0;
    buf->f_files = 2;
    buf->f_ffree = 0;
    buf->f_favail = 0;
    return 0;
}


#endif
