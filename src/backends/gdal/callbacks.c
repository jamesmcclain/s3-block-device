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

#include <gdal.h>
#include <cpl_vsi.h>

#include "../backend.h"

static const char *device_name = "/blocks";

int64_t device_size;
int64_t block_size;
int readonly = 0;
char *blockdir = NULL;
const int PATHLEN = 0x1000;


#include "../common.h"

/*
 * Convert block number to corresponding filename.
 */
static void block_to_filename(uint64_t block_number, char *block_path)
{
    sprintf(block_path, "%s/0x%012lX", blockdir, block_number);
}

int s3bd_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    char block_path[PATHLEN];
    void *buffer_ptr = buf;
    int bytes_to_read = size;
    int current_offset = offset;

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
    char block_path[PATHLEN];
    const void *buffer_ptr = buf;
    int bytes_to_write = size;
    int current_offset = offset;

    while (bytes_to_write > 0) {
        VSILFILE *handle = NULL;
        int64_t block_number = current_offset / block_size;
        int64_t current_offset_in_block = current_offset - (block_size * block_number);
        volatile int64_t bytes_wanted = // Compiler bug?
            MIN(block_size - current_offset_in_block, bytes_to_write);
        int64_t final_offset = current_offset_in_block + bytes_wanted;
        int64_t bytes_at_end = block_size - final_offset;

        block_to_filename(block_number, block_path);

        /* Attempt to open the resource for writing. */
        if ((handle = VSIFOpenL(block_path, "w")) == NULL) {
            return -EIO;
        }

        /* Seeks are forbidden for at least one interesting VSI
           backend (S3), so simulate seeking by reading and writing
           bytes. */
        if (current_offset_in_block != 0) {
            uint8_t *bytes = calloc(1, current_offset_in_block);
            VSILFILE *read_handle = VSIFOpenL(block_path, "r");

            VSIFReadL(bytes, current_offset_in_block, 1, read_handle);
            VSIFCloseL(read_handle);
            if (VSIFWriteL(bytes, current_offset_in_block, 1, handle) != 1) {
                free(bytes);
                VSIFCloseL(handle);
                return -EIO;
            }
            free(bytes);
        }

        /* Write the given bytes into the block. */
        if (VSIFWriteL(buffer_ptr, bytes_wanted, 1, handle) != 1) {
            VSIFCloseL(handle);
            return -EIO;
        }

        /* Write the remaining bytes in the block. */
        if (bytes_at_end > 0) {
            uint8_t *bytes = calloc(1, bytes_at_end);
            VSILFILE *read_handle = VSIFOpenL(block_path, "r");

            VSIFSeekL(read_handle, final_offset, SEEK_SET);
            VSIFReadL(bytes, bytes_at_end, 1, read_handle);
            VSIFCloseL(read_handle);
            if (VSIFWriteL(bytes, bytes_at_end, 1, handle) != 1) {
                free(bytes);
                VSIFCloseL(handle);
                return -EIO;
            }
            free(bytes);
        }

        /* Close the resource. */
        if (VSIFCloseL(handle) == -1) {
            return -EIO;
        }

        /* State */
        buffer_ptr += bytes_wanted;
        current_offset += bytes_wanted;
        bytes_to_write -= bytes_wanted;
    }

    return size;
}
