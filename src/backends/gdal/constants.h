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

#ifndef __CONSTANTS_H__
#define __CONSTANTS_H__

#include <cstdint>
#include <cstddef>

constexpr uint64_t PAGE_SIZE = 0x1000;
constexpr uint64_t PAGE_MASK = (PAGE_SIZE - 1);
constexpr uint64_t PAGES_PER_EXTENT = (1 << 10);
constexpr uint64_t EXTENT_SIZE = PAGE_SIZE * PAGES_PER_EXTENT;
constexpr uint64_t EXTENT_MASK = (EXTENT_SIZE - 1);
constexpr size_t LOCAL_CACHE_DEFAULT_MEGABYTES = 4096;
constexpr size_t EXTENT_BUCKETS = (1 << 8);
constexpr size_t SCRATCH_DESCRIPTORS = (1 << 6);

#define EXTENT_TEMPLATE "%s/%016lX.extent"
#define SCRATCH_TEMPLATE "%s/s3bd.%d"
#define SCRATCH_DEFAULT_DIR "/tmp"
#define S3BD_KEEP_SCRATCH_FILE "S3BD_KEEP_SCRATCH_FILE"
#define S3BD_LOCAL_CACHE_MEGABYTES "S3BD_LOCAL_CACHE_MEGABYTES"
#define S3BD_SCRATCH_DIR "S3BD_SCRATCH_DIR"

#endif
