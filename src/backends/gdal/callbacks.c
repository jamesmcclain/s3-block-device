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
#include "block_range_entry.h"
#include "../backend.h"


static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
const int PATHLEN = 0x100;
const int NUMFILES = 0x100;
static int rtree_initialized = 0;
static long _serial_number = 0;
static VSILFILE *list_handle = NULL;
static pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;


#define NO_S3BD_OPEN
#include "../common.h"
#undef NO_S3BD_OPEN

static void addrs_to_filename(uint64_t start, uint64_t end, long nanos, char *block_path)
{
    sprintf(block_path, "%s/0x%012lX_0x%012lX_%ld", blockdir, start, end, nanos);
}

static uint64_t filename_to_addr(const char *block_path)
{
    uint64_t start;
    char *location = strstr(block_path, "0x");
    sscanf(location + 2, "%012lX", &start);
    return start;
}

void deallocate_intervals(struct file_interval **file_intervals, int num_intervals)
{
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

    file_intervals = malloc(sizeof(struct file_interval) * NUMFILES);

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
            return -EIO;
        } else {
            uint64_t file_start_offset = filename_to_addr(file_interval->filename);
            uint64_t range_start_offset =
                file_interval->start + (file_interval->start_closed ? 0 : 1);
            uint64_t range_end_offset = file_interval->end - (file_interval->end_closed ? 0 : 1);
            uint64_t bytes_wanted = range_end_offset - range_start_offset + 1;
            uint64_t bytes_to_skip_in_file = range_start_offset - file_start_offset;
            uint64_t bytes_to_skip_in_buffer = range_start_offset - offset;

            if (VSIFSeekL(handle, bytes_to_skip_in_file, SEEK_SET) == -1) {
                VSIFCloseL(handle);
                deallocate_intervals(file_intervals, num_files);
                return -EIO;
            } else if (VSIFReadL(buf + bytes_to_skip_in_buffer, bytes_wanted, 1, handle) != 1) {
                VSIFCloseL(handle);
                deallocate_intervals(file_intervals, num_files);
                return -EIO;
            } else {
                VSIFCloseL(handle);
            }
        }
    }

    deallocate_intervals(file_intervals, num_files);

    return size;
}

int s3bd_write(const char *path, const char *buf, size_t size,
               off_t offset, struct fuse_file_info *fi)
{
    char addr_path[PATHLEN];
    VSILFILE *handle = NULL;
    uint64_t addr_start = offset;
    uint64_t addr_end = offset + size - 1;      // closed interval
    long serial_number = ++_serial_number;
    struct block_range_entry bre;

    addrs_to_filename(addr_start, addr_end, serial_number, addr_path);

    // Attempt to open the resource for writing, thekn attempt to
    // write bytes into the file.
    if ((handle = VSIFOpenL(addr_path, "w")) == NULL) {
        return -EIO;
    } else if (VSIFWriteL(buf, size, 1, handle) != 1) {
        VSIFCloseL(handle);
        return -EIO;
    } else {
        VSIFCloseL(handle);
    }

    // Make note of the new block range in the index
    rtree_insert(addr_path, addr_start, addr_end, serial_number);

    // Make note of the new block range in the persistent list
    pthread_mutex_lock(&list_mutex);
    bre.start = addr_start;
    bre.end = addr_end;
    bre.serial_number = serial_number;
    if (VSIFWriteL(&bre, sizeof(bre), 1, list_handle) != 1) {
        pthread_mutex_unlock(&list_mutex);
        return -EIO;
    }
    pthread_mutex_unlock(&list_mutex);

    return size;
}

int s3bd_open(const char *path, struct fuse_file_info *fi)
{
    char list_path[PATHLEN];

    if (strcmp(path, device_name))
        return -ENOENT;

    // Construct the path to the list of block range entries
    sprintf(list_path, "%s/LIST", blockdir);

    // If the R-tree has not been initialized, do so
    if (!rtree_initialized) {
        char addr_path[PATHLEN];
        struct block_range_entry bre;
        VSILFILE *handle = NULL;

        rtree_init();
        handle = VSIFOpenL(list_path, "r");
        if (handle != NULL) {
            while (VSIFReadL(&bre, sizeof(bre), 1, handle) == 1) {
                addrs_to_filename(bre.start, bre.end, bre.serial_number, addr_path);
                rtree_insert(addr_path, bre.start, bre.end, bre.serial_number);
                _serial_number =
                    _serial_number < bre.serial_number ? bre.serial_number : _serial_number;
            }
            VSIFCloseL(handle);
        }
        rtree_initialized = 1;
    }
    // If the append-only handle to the list of entries is not open,
    // open it.
    pthread_mutex_lock(&list_mutex);
    if (list_handle == NULL) {
        if ((list_handle = VSIFOpenL(list_path, "a")) == NULL) {
            VSIFCloseL(list_handle);
            pthread_mutex_unlock(&list_mutex);
            return -EIO;
        }
    }
    pthread_mutex_unlock(&list_mutex);

    return 0;
}
