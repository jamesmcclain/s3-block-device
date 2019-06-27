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
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include "storage.h"
#include "../backend.h"

static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
bool initialized = false;

#define NO_S3BD_OPEN
#define NO_S3BD_FLUSH
#define NO_S3BD_FSYNC
#include "../common.h"
#undef NO_S3BD_FSYNC
#undef NO_S3BD_FLUSH
#undef NO_S3BD_OPEN

int s3bd_read(const char *path,
              char *bytes, size_t size, off_t offset,
              struct fuse_file_info *fi)
{
    return storage_read(offset, size, (uint8_t *)bytes);
}

int s3bd_write(const char *path,
               const char *bytes, size_t size, off_t offset,
               struct fuse_file_info *fi)
{
    return storage_write(offset, size, (uint8_t *)bytes);
}

int s3bd_open(const char *path, struct fuse_file_info *fi)
{
    if (strcmp(path, device_name))
        return -ENOENT;

    if (initialized != true)
    {
        storage_init(blockdir);
        initialized = true;
    }

    return 0;
}

int s3bd_flush(const char *path, struct fuse_file_info *fi)
{
    return storage_flush();
}

int s3bd_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    return s3bd_flush("", fi);
}
