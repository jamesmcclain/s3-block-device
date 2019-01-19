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
#include "block_range_entry.h"


#ifndef __RANGE_H__
#define __RANGE_H__

#ifdef __cplusplus
extern "C" {
#endif

    int rtree_init();
    int rtree_deinit();
    int rtree_insert(uint64_t start, uint64_t end, long nanos);
    int rtree_remove(uint64_t start, uint64_t end, long nanos);
    int rtree_size();
    int rtree_query(struct block_range_entry_part **filenames, int max_results,
                    uint64_t start, uint64_t end);

#ifdef __cplusplus
}
#endif
#endif
