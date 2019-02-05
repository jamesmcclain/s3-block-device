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
#include <stdbool.h>
#include "block_range_entry.h"

#ifndef __RANGE_H__
#define __RANGE_H__

#ifdef __cplusplus
extern "C"
{
#endif

    int rtree_init();
    int rtree_deinit();
    int rtree_insert(uint64_t start, uint64_t end, long sn,
                     bool memory, const uint8_t *bytes);
    int rtree_remove(uint64_t start, uint64_t end, long sn);
    uint64_t rtree_size(bool memory);
    int rtree_query(uint64_t start, uint64_t end, uint8_t *buf,
                    struct block_range_entry_part **parts);
    uint64_t rtree_storage_dump(struct block_range_entry **entries);
    void rtree_memory_mutex_unlock();
    void rtree_memory_clear();
    uint64_t rtree_memory_dump(struct block_range_entry const ***entries,
                               uint8_t const ***bytes);

#ifdef __cplusplus
}
#endif
#endif
