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
constexpr uint64_t EXTENT_SIZE = PAGE_SIZE << 10;
constexpr uint64_t EXTENT_MASK = (EXTENT_SIZE - 1);

extern "C"
{
#endif

    void storage_init(const char *_blockdir);
    void storage_deinit();
    int storage_read(off_t offset, size_t size, uint8_t *bytes);
    int storage_write(off_t offset, size_t size, const uint8_t *bytes);

#ifdef __cplusplus
}

bool extent_read(uint64_t extent_tag);
bool aligned_page_read(uint64_t page_tag, uint16_t size, uint8_t *bytes);
bool aligned_page_write(uint64_t page_tag, uint16_t size, const uint8_t *bytes);
const void *debug_extent_address(uint64_t extent_tag);

#endif
#endif
