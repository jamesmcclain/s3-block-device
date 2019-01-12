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
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include "rtree.h"
#include "../backend.h"


static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
const int PATHLEN = 0x1000;
const int NUMFILES = 0x10000;
static int rtree_initialized = 0;
static struct file_interval **file_intervals = NULL;


#include "../common.h"

/*
 * Convert block number to corresponding filename.
 */
static void block_to_filename(uint64_t block_number, char *block_path)
{
    sprintf(block_path, "%s/0x%012lX", blockdir, block_number);
}

/*
 * Convert starting and ending addresses of range into corresponding
 * filename.
 */
static void addrs_to_filename(uint64_t start, uint64_t end, long nanos, char *block_path)
{
  sprintf(block_path, "%s/0x%012lX_0x%012lX_%ld", blockdir, start, end, nanos);
}

int s3bd_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    void *buffer_ptr = buf;
    int bytes_to_read = size;
    int current_offset = offset;

    if (rtree_initialized && rtree_init()) {
        rtree_initialized = 1;
    }

    while (bytes_to_read > 0) {
        VSILFILE *handle = NULL;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block = current_offset - (block_size * block_number);
        int64_t bytes_wanted = MIN(block_size - current_offset_in_block, bytes_to_read);

        block_to_filename(block_number, block_path);

        /* If block can be found, return the relevant part of its
           contents.  If it cannot be found, return all zeros (that
           part of the virtual block device has not been written to,
           yet). */
        if ((handle = VSIFOpenL(block_path, "r")) == NULL) {
            memset(buffer_ptr, 0, bytes_wanted);        // Resource does not exist (or is not readable)
        } else {
            if ((current_offset_in_block != 0)
                && (VSIFSeekL(handle, current_offset_in_block, SEEK_SET) == -1)) {
                VSIFCloseL(handle);
                return -EIO;    // Resource must be seekable
            } else if (VSIFReadL(buffer_ptr, bytes_wanted, 1, handle) != 1) {
                /* Creation of the handle might have succeed even if
                   the remote asset does not exist. */
                memset(buffer_ptr, 0, bytes_wanted);
            } else if (VSIFCloseL(handle) == -1) {
                return -EIO;    // Resource must be successfully closed
            }
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
    char addr_path[PATHLEN];
    VSILFILE *handle = NULL;
    uint64_t addr_start = offset;
    uint64_t addr_end = offset + size - 1;
    long nanos;
    struct timespec tp;

    // Ensure index has been initialized
    if (rtree_initialized && rtree_init()) {
        rtree_initialized = 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &tp);
    nanos = tp.tv_nsec;

    addrs_to_filename(addr_start, addr_end, nanos, addr_path);

    // Attempt to open the resource for writing
    if ((handle = VSIFOpenL(addr_path, "w")) == NULL) {
        return -EIO;
    }
    // Attempt to write bytes to file
    if (VSIFWriteL(buf, size, 1, handle) != 1) {
        VSIFCloseL(handle);
        return -EIO;
    }
    // Note new range in index, close the resource
    rtree_insert(addr_path, addr_start, addr_end, nanos);
    VSIFCloseL(handle);

    return size;
}
