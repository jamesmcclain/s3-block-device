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

#include <cstdint>
#include <iostream>
#include "block_range_entry.h"


block_range_entry::block_range_entry():
    start(0), end(0), serial_number(-1) {}

block_range_entry::block_range_entry(uint64_t _start, uint64_t _end, long _sn):
    start(_start), end(_end), serial_number(_sn) {}

block_range_entry::block_range_entry(const block_range_entry & rhs)
{
    start = rhs.start;
    end = rhs.end;
    serial_number = rhs.serial_number;
}

block_range_entry & block_range_entry::operator=(const block_range_entry & rhs)
{
    start = rhs.start;
    end = rhs.end;
    serial_number = rhs.serial_number;
    return *this;
}

block_range_entry & block_range_entry::operator+=(const block_range_entry & rhs)
{
    if (serial_number < rhs.serial_number) {
        this->operator=(rhs);
    }
    return *this;
}

std::ostream & operator<<(std::ostream &out, const block_range_entry & entry)
{
    out << std::hex << std::uppercase
        << "block_range_entry("
        << "start=0x" << entry.start
        << ",end=0x" << entry.end
        << std::nouppercase << std::dec
        << ",serial_number=" << entry.serial_number
        << ")";
    return out;
}

block_range_entry_part::block_range_entry_part(const block_range_entry & _entry,
                                               uint64_t _start, uint64_t _end):
  entry(_entry), start(_start), end(_end) {}

bool operator==(const block_range_entry & lhs, const block_range_entry & rhs)
{
    return ((lhs.start == rhs.start) && (lhs.end == rhs.end)
            && (lhs.serial_number == rhs.serial_number));
}

bool operator==(const block_range_entry_part & lhs, const block_range_entry_part & rhs)
{
    return ((lhs.entry == rhs.entry) && (lhs.end == rhs.end));
}

std::ostream & operator<<(std::ostream &out, const block_range_entry_part & entry_part)
{
    out << "block_range_entry_part("
        << "entry=[" << entry_part.entry
        << std::hex << std::uppercase
        << ",start=0x" << entry_part.start
        << ",end=0x" << entry_part.end
        << ")"
        << std::nouppercase << std::dec;
    return out;
}
