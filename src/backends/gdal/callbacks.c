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
#include <pthread.h>

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
const int NUMFILES = 0x100;
static int rtree_initialized = 0;
static long _nanos = 0;
static pthread_mutex_t gdal_mutex = PTHREAD_MUTEX_INITIALIZER;


#include "../common.h"

static void addrs_to_filename(uint64_t start, uint64_t end, long nanos, char *block_path)
{
  sprintf(block_path, "%s/0x%012lX_0x%012lX_%ld", blockdir, start, end, nanos);
}

static uint64_t filename_to_addr(const char * block_path)
{
  uint64_t start;
  char * location = strstr(block_path, "0x");
  sscanf(location + 2, "%012lX", &start);
  return start;
}

void deallocate_intervals(struct file_interval ** file_intervals, int num_intervals) {
  for (int i = 0; i < num_intervals; ++i) {
    free(file_intervals[i]->filename);
    free(file_intervals[i]);
  }
  free(file_intervals);
}

int s3bd_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int num_files = 0;
    uint64_t addr_start = offset;
    uint64_t addr_end = offset + size - 1;      // closed interval
    struct file_interval **file_intervals = NULL;

    pthread_mutex_lock(&gdal_mutex);
    // Ensure that index has been initialized
    if (rtree_initialized || rtree_init()) {
        rtree_initialized = 1;
    }

    // Ensure that interval list has been initialized
    if (file_intervals == NULL) {
        file_intervals = malloc(sizeof(struct file_interval) * NUMFILES);
    }

    // Get paths covering this range
    num_files = rtree_query(file_intervals, NUMFILES, addr_start, addr_end);

    // Clear the buffer.  Uncovered bytes are assumed to be zero.
    memset(buf, 0, size);

    // For each file covering the range, copy the appropriate portion
    // into the buffer.
    for (int i = 0; i < num_files; ++i) {
        VSILFILE *handle = NULL;
        struct file_interval *file_interval = file_intervals[i];

        if ((handle = VSIFOpenL(file_interval->filename, "r")) == NULL) {
            VSIFCloseL(handle);
            deallocate_intervals(file_intervals, num_files);
            pthread_mutex_unlock(&gdal_mutex);
            return -EIO;
        } else {
            uint64_t file_start_offset = filename_to_addr(file_interval->filename);
            uint64_t range_start_offset =
                file_interval->start + (file_interval->start_closed ? 0 : 1);
            uint64_t range_end_offset = file_interval->end - (file_interval->end_closed ? 0 : 1);
            uint64_t bytes_wanted = range_end_offset - range_start_offset + 1;
            uint64_t bytes_to_skip_in_file = range_start_offset - file_start_offset;
            uint64_t bytes_to_skip_in_buffer = range_start_offset - offset;

            fprintf(stderr, "XXX %s %ld %ld %ld %ld %ld %ld\n", file_interval->filename, file_start_offset, range_start_offset, range_end_offset, bytes_wanted, bytes_to_skip_in_file, bytes_to_skip_in_buffer);
            if (VSIFSeekL(handle, bytes_to_skip_in_file, SEEK_SET) == -1) {
                VSIFCloseL(handle);
                deallocate_intervals(file_intervals, num_files);
                pthread_mutex_unlock(&gdal_mutex);
                return -EIO;
            } else if (VSIFReadL(buf + bytes_to_skip_in_buffer, bytes_wanted, 1, handle) != 1) {
                VSIFCloseL(handle);
                deallocate_intervals(file_intervals, num_files);
                pthread_mutex_unlock(&gdal_mutex);
                return -EIO;
            } else {
                VSIFCloseL(handle);
            }
        }
    }

    deallocate_intervals(file_intervals, num_files);
    pthread_mutex_unlock(&gdal_mutex);
    return size;
}

int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
    char addr_path[PATHLEN];
    VSILFILE *handle = NULL;
    uint64_t addr_start = offset;
    uint64_t addr_end = offset + size - 1; // closed interval
    long nanos;

    // Ensure that index has been initialized
    if (rtree_initialized || rtree_init()) {
        rtree_initialized = 1;
    }

    nanos = _nanos++;

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
