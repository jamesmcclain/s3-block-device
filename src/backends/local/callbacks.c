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
#include <errno.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "../backend.h"

static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
const int PATHLEN = 0x1000;

#define MIN(a, b) ((a) > (b) ? (b) : (a))

#include "../common.h"

/*
 * Convert block number to corresponding filename.
 */
static void block_to_filename(uint64_t block_number, char *block_path)
{
    sprintf(block_path, "%s/0x%012lX", blockdir, block_number);
}

void fullwrite(int fd, const void *buffer, int bytes)
{
    int sent = 0;

    while (bytes - sent > 0)
    {
        int i = write(fd, buffer + sent, bytes - sent);
        if (i < 0)
            break;
        sent += i;
    }
}

void fullread(int fd, void *buffer, int bytes)
{
    int recvd = 0, i = 0;

    while (1)
    {
        i = read(fd, buffer + recvd, bytes - recvd);
        if (i < 0)
            break;
        if ((i <= bytes - recvd) || (recvd == bytes))
            break;
        recvd += i;
    }
}

int s3bd_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    void *buffer_ptr = buf;
    int bytes_to_read = size;
    int current_offset = offset;

    while (bytes_to_read > 0)
    {
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block = current_offset - (block_size * block_number);
        int64_t bytes_wanted = MIN(block_size - current_offset_in_block, bytes_to_read);

        block_to_filename(block_number, block_path);

        /* If block can be found, return the relevant part of its
           contents.  If it cannot be found, return all zeros (that
           part of the virtual block device has not been written to,
           yet). */
        if (access(block_path, R_OK) != -1)
        { // File exists and is readable
            int fd = open(block_path, O_RDONLY);
            lseek(fd, current_offset_in_block, SEEK_SET);
            fullread(fd, buffer_ptr, bytes_wanted);
            close(fd);
        }
        else
        { // File does not exist (or is not readable)
            memset(buffer_ptr, 0, bytes_wanted);
        }

        /* State */
        buffer_ptr += bytes_wanted;
        current_offset += bytes_wanted;
        bytes_to_read -= bytes_wanted;
    }

    return size;
}

int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    const void *buffer_ptr = buf;
    int bytes_to_write = size;
    int current_offset = offset;

    while (bytes_to_write > 0)
    {
        int fd;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block = current_offset - (block_size * block_number);
        volatile int64_t bytes_wanted = // Compiler bug?
            MIN(block_size - current_offset_in_block, bytes_to_write);

        block_to_filename(block_number, block_path);

        /* Get a file descriptor that points to the appropriate
           block. Either open an existing one, or create one of the
           appropriate size. */
        if (access(block_path, W_OK) != -1)
        { // File exists and is writable
            fd = open(block_path, O_RDWR);
            lseek(fd, current_offset_in_block, SEEK_SET);
        }
        else if (access(block_path, F_OK) == -1)
        { // File does not exist, make it
            fd = open(block_path, O_RDWR | O_CREAT);
            ftruncate(fd, block_size);
            lseek(fd, current_offset_in_block, SEEK_SET);
        }
        else
        { // Evidently the file exists, but is not writable
            close(fd);
            return -EIO;
        }

        /* Write the block. */
        fullwrite(fd, buffer_ptr, bytes_to_write);
        close(fd);

        buffer_ptr += bytes_wanted;
        current_offset += bytes_wanted;
        bytes_to_write -= bytes_wanted;
    }

    return size;
}
