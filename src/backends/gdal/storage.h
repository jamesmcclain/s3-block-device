/*
 * The MIT License
 *
 * Copyright (c) 2019 James McClain
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

#include <stdint.h>

#ifndef __STORAGE_H__
#define __STORAGE_H__

#ifdef __cplusplus
constexpr uint64_t PAGE_SIZE = 0x1000;
constexpr uint64_t PAGE_MASK = (PAGE_SIZE - 1);
constexpr uint64_t PAGES_PER_EXTENT = (1 << 10);
constexpr uint64_t EXTENT_SIZE = PAGE_SIZE * PAGES_PER_EXTENT;
constexpr uint64_t EXTENT_MASK = (EXTENT_SIZE - 1);
constexpr uint64_t LOCAL_CACHE_MEGABYTES = 4096;
constexpr int APPROX_MAX_BACKGROUND_THREADS = 16;

#define EXTENT_TEMPLATE "%s/%016lX.extent"
#define SCRATCH_TEMPLATE "/tmp/s3bd.%d"
#define S3BD_KEEP_SCRATCH_FILE "S3BD_KEEP_SCRATCH_FILE"
#define S3BD_LOCAL_CACHE_MEGABYTES "S3BD_LOCAL_CACHE_MEGABYTES"

extern "C"
{
#endif

    void storage_init(const char *_blockdir);
    void storage_deinit();
    int storage_flush();
    int storage_read(off_t offset, size_t size, uint8_t *bytes);
    int storage_write(off_t offset, size_t size, const uint8_t *bytes);

#ifdef __cplusplus
}

bool aligned_page_read(uint64_t page_tag, uint16_t size, uint8_t *bytes, bool should_report = true);
bool aligned_whole_page_write(uint64_t page_tag, const uint8_t *bytes);

#endif
#endif
