/*
 * The MIT License
 *
 * Copyright (c) 2018-2019 James McClain
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

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <deque>
#include <exception>
#include <vector>
#include <pthread.h>

// https://www.boost.org/doc/libs/1_69_0/libs/geometry/doc/html/geometry/spatial_indexes/rtree_examples/index_stored_in_mapped_file_using_boost_interprocess.html
// https://www.boost.org/doc/libs/1_69_0/libs/geometry/doc/html/geometry/spatial_indexes/rtree_examples.html
// https://www.boost.org/doc/libs/1_69_0/libs/geometry/doc/html/geometry/reference/spatial_indexes/boost__geometry__index__rtree.html
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>

// https://www.boost.org/doc/libs/1_69_0/libs/icl/doc/html/index.html
#include <boost/icl/interval_map.hpp>

#include "storage.h"
#include "block_range_entry.h"

// Byte Vector
typedef std::vector<uint8_t> byte_vector_t;
typedef std::pair<struct block_range_entry, byte_vector_t> range_of_bytes_t;
typedef std::vector<range_of_bytes_t> vector_of_ranges_of_bytes_t;

// R-tree
namespace bi = boost::interprocess;
namespace bg = boost::geometry;
namespace bgm = boost::geometry::model;
namespace bgi = boost::geometry::index;
typedef bgm::point<uint64_t, 1, bg::cs::cartesian> point_t;
typedef bgm::box<point_t> range_t;
typedef std::pair<range_t, struct block_range_entry> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;

// Intervals
namespace icl = boost::icl;
typedef icl::interval_map<uint64_t, block_range_entry> secondary_storage_map_t;
typedef icl::interval<uint64_t> addr_interval_t;

// Memory
static vector_of_ranges_of_bytes_t *memory_ptr = nullptr;
static pthread_rwlock_t memory_lock = PTHREAD_RWLOCK_INITIALIZER;

// Secondary Storage
static rtree_t *secondary_storage_rtree_ptr = nullptr;
static pthread_rwlock_t secondary_storage_rtree_lock = PTHREAD_RWLOCK_INITIALIZER;

/**
 * Initialize storage.
 */
extern "C" int storage_init()
{
    if (secondary_storage_rtree_ptr == nullptr)
    {
        secondary_storage_rtree_ptr = new rtree_t();
    }
    if (memory_ptr == nullptr)
    {
        memory_ptr = new vector_of_ranges_of_bytes_t();
    }

    if (secondary_storage_rtree_ptr == nullptr || memory_ptr == nullptr)
    {
        throw std::bad_alloc();
    }

    return 1;
}

/**
 * Deinitialize storage.
 */
extern "C" int storage_deinit()
{
    if (secondary_storage_rtree_ptr != nullptr)
    {
        delete secondary_storage_rtree_ptr;
        secondary_storage_rtree_ptr = nullptr;
    }
    if (memory_ptr != nullptr)
    {
        delete memory_ptr;
        memory_ptr = nullptr;
    }
    return 1;
}

/**
 * Insert the bytes into the in-memory cache.
 *
 * @param start The starting offset of of the byte range
 * @param end The ending offset of the byte range (inclusive)
 * @param sn The serial number of the range of bytes
 * @param bytes The range of bytes to insert
 * @return The number of ranges in the in-memory cache
 */
static int insert_into_memory(uint64_t start, uint64_t end, long sn,
                              const uint8_t *bytes) noexcept
{
    assert(start <= end);

    auto entry = block_range_entry(start, end, sn);
    auto vbytes = byte_vector_t(end - start + 1);
    auto range_of_bytes = std::make_pair(entry, vbytes);
    int size = 0;

    vbytes.insert(range_of_bytes.second.begin(), bytes, bytes + (end - start + 1));
    pthread_rwlock_wrlock(&memory_lock);
    memory_ptr->push_back(std::move(range_of_bytes));
    size = memory_ptr->size();
    pthread_rwlock_unlock(&memory_lock);

    return size;
}

/**
 * Insert an entry into the secondary-storage R-Tree.
 *
 * @param start The starting offset of the entry
 * @param end The ending offset of the entry (inclusive)
 * @param sn The serial number of the entry
 * @return The number of items in the R-Tree
 */
static int insert_into_storage(uint64_t start, uint64_t end, long sn) noexcept
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = block_range_entry(start, end, sn);
    auto value = std::make_pair(range, entry);
    int size = 0;

    pthread_rwlock_wrlock(&secondary_storage_rtree_lock);
    secondary_storage_rtree_ptr->insert(std::move(value));
    size = secondary_storage_rtree_ptr->size();
    pthread_rwlock_unlock(&secondary_storage_rtree_lock);

    return size;
}

/**
 * Insert a new range of bytes into the in-memory cache, or a new
 * entry into secondary-storage R-Tree.  This could and perhaps should
 * be two completely separate functions.
 *
 * @param start The starting offset of the range or entry
 * @param end The ending offset of the range or entry (inclusive)
 * @param sn The serial number of the entry
 * @param memory If true store to memory, if false store to secondary storage
 * @param bytes The bytes to be written (ignored if memory != true)
 * @return The number of ranges (resp. entries) in the cache (resp. storage)
 */
extern "C" int storage_insert(uint64_t start, uint64_t end, long sn,
                              bool memory, const uint8_t *bytes)
{
    if (memory)
    {
        return insert_into_memory(start, end, sn, bytes);
    }
    else
    {
        return insert_into_storage(start, end, sn);
    }
}

extern "C" int storage_remove(uint64_t start, uint64_t end, long sn)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_sequence_t());
    auto value = std::make_pair(range, entry);

    pthread_rwlock_wrlock(&secondary_storage_rtree_lock);
    secondary_storage_rtree_ptr->remove(value);
    pthread_rwlock_unlock(&secondary_storage_rtree_lock);
    return secondary_storage_rtree_ptr->size();
}

extern "C" uint64_t rtree_size(bool memory)
{
    uint64_t size;
    rtree_t *rtree_ptr = nullptr;
    pthread_rwlock_t *lock_ptr;

    if (memory)
    {
        rtree_ptr = memory_ptr;
        lock_ptr = &memory_lock;
    }
    else
    {
        rtree_ptr = secondary_storage_rtree_ptr;
        lock_ptr = &secondary_storage_rtree_lock;
    }

    pthread_rwlock_rdlock(lock_ptr);
    size = static_cast<uint64_t>(rtree_ptr->size());
    pthread_rwlock_unlock(lock_ptr);

    return size;
}

extern "C" int rtree_query(uint64_t start, uint64_t end, uint8_t *buf,
                           block_range_entry_part **parts)
{
    auto range = range_t(point_t(start), point_t(end));
    auto intersects = bgi::intersects(range);
    auto storage_candidates = std::vector<value_t>();
    auto memory_candidates = std::vector<value_t>();
    secondary_storage_map_t file_map;

    pthread_rwlock_rdlock(&memory_lock);

    // Read relevant ranges of bytes from in-memory data structure
    memory_ptr->query(intersects, std::back_inserter(memory_candidates));

    // Copy ranges of bytes into the provided return buffer
    if (buf != nullptr)
    {
        for (auto itr = memory_candidates.begin(); itr != memory_candidates.end(); ++itr)
        {
            auto entry = itr->second.first;
            auto byte_sequence = itr->second.second;
            const uint64_t intersection_start = std::max(entry.start, start);
            const uint64_t skip_in_buffer = intersection_start <= start ? start - intersection_start : 0;
            uint64_t skip_in_sequence = intersection_start <= entry.start ? entry.start - intersection_start : 0;
            auto buffer_begin = buf + skip_in_buffer;

            for (const auto &byte_vector : byte_sequence)
            {
                if (skip_in_sequence >= byte_vector.size())
                {
                    skip_in_sequence -= byte_vector.size();
                    continue;
                }
                else //if (skip_in_sequence < byte_vector.size())
                {
                    std::copy(
                        byte_vector.begin() + skip_in_sequence,
                        byte_vector.end(),
                        buffer_begin);
                    buffer_begin += (byte_vector.size() - skip_in_sequence);
                    skip_in_sequence = 0;
                }
            }
        }
    }

    pthread_rwlock_rdlock(&secondary_storage_rtree_lock);

    // Query for relevant ranges of external-storage bytes
    secondary_storage_rtree_ptr->query(intersects, std::back_inserter(storage_candidates));

    // Insert results from the R-tree query into the interval map
    // XXX insert directly into the file_map?
    for (auto itr = storage_candidates.begin(); itr != storage_candidates.end(); ++itr)
    {
        uint64_t interval_start = itr->first.min_corner().get<0>();
        uint64_t interval_end = itr->first.max_corner().get<0>();
        auto addr_interval = addr_interval_t::closed(interval_start, interval_end);
        auto pair = std::make_pair(addr_interval, itr->second.first);

        file_map += pair;
    }

    // Subtract byte ranges found in the in-memory structure from the
    // list of intervals that must be read from external storage.
    for (auto itr = memory_candidates.begin(); itr != memory_candidates.end(); ++itr)
    {
        auto range = itr->first;
        uint64_t interval_start = range.min_corner().get<0>();
        uint64_t interval_end = range.max_corner().get<0>();
        auto addr_interval = addr_interval_t::closed(interval_start, interval_end);

        file_map -= addr_interval;
    }

    pthread_rwlock_unlock(&memory_lock);

    // Allocate the return array for the list of ranges stored on
    // external-storage.
    uint64_t num_files = icl::interval_count(file_map);

    *parts = static_cast<block_range_entry_part *>(malloc(sizeof(block_range_entry_part) * num_files));
    if (*parts == nullptr)
    {
        throw std::bad_alloc();
    }

    // Copy resulting intervals into the return array
    int i = 0;
    for (auto itr = file_map.begin(); itr != file_map.end(); ++itr)
    {
        auto addr_interval = itr->first;
        uint64_t interval_start = std::max(addr_interval.lower(), start);
        uint64_t interval_end = std::min(addr_interval.upper(), end);
        auto entry = itr->second;

        // Close the interval
        if (!icl::contains(addr_interval, interval_start))
        {
            interval_start += 1;
        }
        if (!icl::contains(addr_interval, interval_end))
        {
            interval_end -= 1;
        }

        if (interval_start <= interval_end)
        {
            auto part = block_range_entry_part(entry, interval_start, interval_end);
            (*parts)[i++] = part;
        }
    }

    pthread_rwlock_unlock(&secondary_storage_rtree_lock);

    return i;
}

extern "C" uint64_t rtree_storage_dump(block_range_entry **entries)
{
    uint64_t n, i;

    pthread_rwlock_rdlock(&secondary_storage_rtree_lock);

    n = i = static_cast<uint64_t>(secondary_storage_rtree_ptr->size());
    *entries = static_cast<struct block_range_entry *>(
        malloc(sizeof(struct block_range_entry) * n));
    if (*entries == nullptr)
    {
        throw std::bad_alloc();
    }

    for (auto itr = secondary_storage_rtree_ptr->begin(); itr != secondary_storage_rtree_ptr->end(); ++itr)
    {
        (*entries)[--i] = itr->second.first;
    }

    pthread_rwlock_unlock(&secondary_storage_rtree_lock);

    return n;
}

extern "C" uint64_t rtree_memory_dump(struct block_range_entry **entries,
                                      uint8_t **bytes)
{
    uint64_t n = 0;
    uint64_t i = 0;
    uint64_t buffer_size = 0;
    uint8_t *byte_ptr = nullptr;

    pthread_rwlock_wrlock(&memory_lock);

    for (auto itr = memory_ptr->begin(); itr != memory_ptr->end(); ++itr)
    {
        const auto &byte_sequence = itr->second.second;
        for (const auto &byte_vector : byte_sequence)
        {
            buffer_size += byte_vector.size();
        }
    }

    n = memory_ptr->size();
    *entries = static_cast<struct block_range_entry *>(
        malloc(sizeof(struct block_range_entry) * n));
    *bytes = byte_ptr = static_cast<uint8_t *>(
        malloc(sizeof(uint8_t) * buffer_size));
    if (*entries == nullptr || *bytes == nullptr)
    {
        throw std::bad_alloc();
    }

    for (auto itr = memory_ptr->begin(); itr != memory_ptr->end(); ++itr)
    {
        const auto &entry = itr->second.first;
        const auto &byte_sequence = itr->second.second;

        (*entries)[i++] = entry;
        for (const auto &byte_vector : byte_sequence)
        {
            std::copy(byte_vector.begin(), byte_vector.end(), byte_ptr);
            byte_ptr += byte_vector.size();
        }
    }

    memory_ptr->clear();

    pthread_rwlock_unlock(&memory_lock);

    return n;
}
