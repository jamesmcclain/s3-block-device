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
#ifdef __cplusplus
#include <iostream>
#endif

#ifndef __BLOCK_RANGE_ENTRY_H__
#define __BLOCK_RANGE_ENTRY_H__

struct block_range_entry {
    uint64_t start;
    uint64_t end;
    long serial_number;

#ifdef __cplusplus
    block_range_entry();
    block_range_entry(uint64_t _start, uint64_t _end, long _sn);
    block_range_entry(const block_range_entry & rhs);
    block_range_entry & operator=(const block_range_entry & rhs);
    block_range_entry & operator+=(const block_range_entry & rhs);

    friend std::ostream & operator<<(std::ostream &out, const block_range_entry & entry);
#endif
};

struct block_range_entry_part {
    struct block_range_entry entry;
    // Closed interval
    uint64_t start;
    uint64_t end;

#ifdef __cplusplus
    block_range_entry_part(const block_range_entry & entry,
                           uint64_t start, uint64_t end);

    friend std::ostream & operator<<(std::ostream &out, const block_range_entry_part & entry_part);
#endif
};

#ifdef __cplusplus
bool operator==(const block_range_entry & lhs, const block_range_entry & rhs);
bool operator==(const block_range_entry_part & lhs, const block_range_entry_part & rhs);
#endif

#endif
