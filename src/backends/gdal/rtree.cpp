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

#include "rtree.h"
#include "block_range_entry.h"

// R-tree
namespace bi = boost::interprocess;
namespace bg = boost::geometry;
namespace bgm = boost::geometry::model;
namespace bgi = boost::geometry::index;
typedef bgm::point<uint64_t, 1, bg::cs::cartesian> point_t;
typedef bgm::box<point_t> range_t;
typedef std::vector<uint8_t> byte_vector_t;
typedef std::pair<range_t, std::pair<struct block_range_entry, byte_vector_t>> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;

// Intervals
namespace icl = boost::icl;
typedef icl::interval_map<uint64_t, block_range_entry> file_map_t;
typedef icl::interval<uint64_t> addr_interval_t;

static rtree_t *storage_rtree_ptr = nullptr;
static rtree_t *memory_rtree_ptr = nullptr;
static pthread_rwlock_t storage_rtree_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t memory_rtree_lock = PTHREAD_RWLOCK_INITIALIZER;

extern "C" int rtree_init()
{
    if (storage_rtree_ptr == nullptr)
    {
        storage_rtree_ptr = new rtree_t();
    }
    if (memory_rtree_ptr == nullptr)
    {
        memory_rtree_ptr = new rtree_t();
    }

    if (storage_rtree_ptr == nullptr || memory_rtree_ptr == nullptr)
    {
        throw std::bad_alloc();
    }

    return 1;
}

extern "C" int rtree_deinit()
{
    if (storage_rtree_ptr != nullptr)
    {
        delete storage_rtree_ptr;
        storage_rtree_ptr = nullptr;
    }
    if (memory_rtree_ptr != nullptr)
    {
        delete memory_rtree_ptr;
        memory_rtree_ptr = nullptr;
    }
    return 1;
}

static int rtree_insert_memory(uint64_t start, uint64_t end, long sn,
                               const uint8_t *bytes) noexcept
{
    auto query_range = range_t(point_t(start > 0 ? start - 1 : 0), point_t(end + 1));
    auto byte_vector = byte_vector_t();
    auto intersects = bgi::intersects(query_range);
    auto candidates = std::vector<value_t>();
    uint64_t num_bytes = end - start + 1;
    int size;

    if (bytes != nullptr)
    {
        byte_vector.insert(byte_vector.end(), bytes, bytes + num_bytes);
    }
    else
    {
        byte_vector.resize(num_bytes);
    }

    pthread_rwlock_wrlock(&memory_rtree_lock);

    memory_rtree_ptr->query(intersects, std::back_inserter(candidates));

    for (auto itr = candidates.begin(); itr != candidates.end(); ++itr)
    {
        uint64_t old_start = itr->first.min_corner().get<0>();
        uint64_t old_end = itr->first.max_corner().get<0>();
        auto old_byte_vector = itr->second.second;

        // If the old range begins strictly before the new one,
        // then bytes from the old range must be added to the
        // beginning of this range.
        if (old_start < start)
        {
            uint64_t bytes_needed = start - old_start;
            auto old_begin = old_byte_vector.begin();

            // https://en.cppreference.com/w/cpp/iterator/move_iterator
            // https://en.cppreference.com/w/cpp/container/deque
            byte_vector.insert(
                byte_vector.begin(),
                std::move_iterator<byte_vector_t::iterator>(old_begin),
                std::move_iterator<byte_vector_t::iterator>(old_begin + bytes_needed));
            start = old_start;
        }

        // If the old range ends strictly after the new one, then
        // bytes from the old range must be appended to the end of
        // this range.
        if (end < old_end)
        {
            uint64_t bytes_needed = old_end - end;
            auto old_fin = old_byte_vector.end();

            byte_vector.insert(
                byte_vector.end(),
                std::move_iterator<byte_vector_t::iterator>(old_fin - bytes_needed),
                std::move_iterator<byte_vector_t::iterator>(old_fin));
            end = old_end;
        }
    }

    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector);
    auto value = std::make_pair(range, entry);

    memory_rtree_ptr->remove(candidates.begin(), candidates.end());
    memory_rtree_ptr->insert(value);
    size = memory_rtree_ptr->size();

    pthread_rwlock_unlock(&memory_rtree_lock);

    return size;
}

static int rtree_insert_storage(uint64_t start, uint64_t end, long sn) noexcept
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector_t());
    auto value = std::make_pair(range, entry);
    int size;

    pthread_rwlock_wrlock(&storage_rtree_lock);
    storage_rtree_ptr->insert(value);
    size = storage_rtree_ptr->size();
    pthread_rwlock_unlock(&storage_rtree_lock);

    return size;
}

extern "C" int rtree_insert(uint64_t start, uint64_t end, long sn,
                            bool memory, const uint8_t *bytes)
{
    if (memory)
    {
        return rtree_insert_memory(start, end, sn, bytes);
    }
    else
    {
        return rtree_insert_storage(start, end, sn);
    }
}

extern "C" int rtree_remove(uint64_t start, uint64_t end, long sn)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector_t());
    auto value = std::make_pair(range, entry);

    pthread_rwlock_wrlock(&storage_rtree_lock);
    storage_rtree_ptr->remove(value);
    pthread_rwlock_unlock(&storage_rtree_lock);
    return storage_rtree_ptr->size();
}

extern "C" uint64_t rtree_size(bool memory)
{
    uint64_t size;
    rtree_t *rtree_ptr = nullptr;
    pthread_rwlock_t *lock_ptr;

    if (memory)
    {
        rtree_ptr = memory_rtree_ptr;
        lock_ptr = &memory_rtree_lock;
    }
    else
    {
        rtree_ptr = storage_rtree_ptr;
        lock_ptr = &storage_rtree_lock;
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
    file_map_t file_map;

    pthread_rwlock_rdlock(&memory_rtree_lock);

    // Read relevant ranges of bytes from in-memory data structure
    memory_rtree_ptr->query(intersects, std::back_inserter(memory_candidates));

    // Copy ranges of bytes into the provided return buffer
    if (buf != nullptr)
    {
        for (auto itr = memory_candidates.begin(); itr != memory_candidates.end(); ++itr)
        {
            auto entry = itr->second.first;
            auto byte_vector = itr->second.second;
            uint64_t intersection_start = std::max(entry.start, start);
            uint64_t intersection_end = std::min(entry.end, end);
            uint64_t skip_in_vector = intersection_start <= entry.start ? entry.start - intersection_start : 0;
            uint64_t skip_in_buffer = intersection_start <= start ? start - intersection_start : 0;
            auto vector_begin = byte_vector.begin() + skip_in_vector;
            auto vector_end = vector_begin + (intersection_end - intersection_start + 1);
            auto buffer_begin = buf + skip_in_buffer;

            std::copy(vector_begin, vector_end, buffer_begin);
        }
    }

    pthread_rwlock_rdlock(&storage_rtree_lock);

    // Query for relevant ranges of external-storage bytes
    storage_rtree_ptr->query(intersects, std::back_inserter(storage_candidates));

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

    pthread_rwlock_unlock(&memory_rtree_lock);

    // Allocate the return array for the list of ranges stored on
    // external-storage.
    uint64_t num_files = icl::interval_count(file_map);

    *parts = static_cast<block_range_entry_part *>(malloc(sizeof(block_range_entry_part) * num_files));
    if (*parts == nullptr)
    {
        exit(-1);
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

    pthread_rwlock_unlock(&storage_rtree_lock);

    return i;
}

extern "C" uint64_t rtree_storage_dump(block_range_entry **entries)
{
    uint64_t n, i;

    pthread_rwlock_rdlock(&storage_rtree_lock);
    n = i = static_cast<uint64_t>(storage_rtree_ptr->size());
    *entries = static_cast<struct block_range_entry *>(
        malloc(sizeof(struct block_range_entry) * n));
    for (auto itr = storage_rtree_ptr->begin(); itr != storage_rtree_ptr->end(); ++itr)
    {
        (*entries)[--i] = itr->second.first;
    }
    pthread_rwlock_unlock(&storage_rtree_lock);
    return n;
}

extern "C" uint64_t rtree_memory_dump(struct block_range_entry **entries,
                                      const uint8_t ***bytes)
{
    uint64_t n, i;

    pthread_rwlock_wrlock(&memory_rtree_lock);
    n = i = memory_rtree_ptr->size();
    *entries = static_cast<struct block_range_entry *>(
        malloc(sizeof(struct block_range_entry) * n));
    *bytes = static_cast<uint8_t const **>(
        malloc(sizeof(uint8_t *) * n));
    if (entries == nullptr || bytes == nullptr)
    {
        throw std::bad_alloc();
    }

    for (auto itr = memory_rtree_ptr->begin(); itr != memory_rtree_ptr->end(); ++itr)
    {
        const auto &byte_vector = itr->second.second;
        uint8_t *byte_ptr = static_cast<uint8_t *>(malloc(sizeof(uint8_t) * byte_vector.size()));

        if (byte_ptr == nullptr)
        {
            throw std::bad_alloc();
        }
        (*entries)[--i] = itr->second.first;
        (*bytes)[i] = byte_ptr;
        std::copy(byte_vector.begin(), byte_vector.end(), byte_ptr); // XXX copying
    }
    memory_rtree_ptr->clear();
    pthread_rwlock_unlock(&memory_rtree_lock);

    return n;
}
