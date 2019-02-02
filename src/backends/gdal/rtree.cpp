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
#include <vector>
#include <algorithm>
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
typedef std::pair<range_t, std::pair<block_range_entry, byte_vector_t>> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;

// Intervals
namespace icl = boost::icl;
typedef icl::interval_map<uint64_t, block_range_entry> file_map_t;
typedef icl::interval<uint64_t> addr_interval_t;

static rtree_t *clean_rtree_ptr = nullptr;
static rtree_t *dirty_rtree_ptr = nullptr;
static pthread_rwlock_t clean_rtree_lock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_rwlock_t dirty_rtree_lock = PTHREAD_RWLOCK_INITIALIZER;

extern "C" int rtree_init()
{
    if (clean_rtree_ptr == nullptr)
    {
        clean_rtree_ptr = new rtree_t();
    }
    if (dirty_rtree_ptr == nullptr)
    {
        dirty_rtree_ptr = new rtree_t();
    }
    return 1;
}

extern "C" int rtree_deinit()
{
    if (clean_rtree_ptr != nullptr)
    {
        delete clean_rtree_ptr;
        clean_rtree_ptr = nullptr;
    }
    if (dirty_rtree_ptr != nullptr)
    {
        delete dirty_rtree_ptr;
        dirty_rtree_ptr = nullptr;
    }
    return 1;
}

static int rtree_insert_dirty(uint64_t start, uint64_t end, long sn, uint8_t *bytes)
{
    auto query_range = range_t(point_t(start), point_t(end));
    auto byte_vector = byte_vector_t();
    auto intersects = bgi::intersects(query_range);
    auto candidates = std::vector<value_t>();
    uint64_t num_bytes = end - start + 1;
    int size;

    byte_vector.insert(byte_vector.end(), bytes, bytes + num_bytes);

    pthread_rwlock_wrlock(&dirty_rtree_lock);
    {
        dirty_rtree_ptr->query(intersects, std::back_inserter(candidates));
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

                byte_vector.insert(byte_vector.begin(), old_begin, old_begin + bytes_needed);
                start = old_start;
            }

            // If the old range ends strictly after the new one, then
            // bytes from the old range must be appended to the end of
            // this range.
            if (end < old_end)
            {
                uint64_t bytes_needed = old_end - end;
                auto old_fin = old_byte_vector.end();

                byte_vector.insert(byte_vector.end(), old_fin - bytes_needed, old_fin);
                end = old_end;
            }
        }

        auto range = range_t(point_t(start), point_t(end));
        auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector);
        auto value = std::make_pair(range, entry);

        dirty_rtree_ptr->remove(candidates.begin(), candidates.end());
        dirty_rtree_ptr->insert(value);
        size = dirty_rtree_ptr->size();
    }
    pthread_rwlock_unlock(&dirty_rtree_lock);

    return size;
}

static int rtree_insert_clean(uint64_t start, uint64_t end, long sn)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector_t());
    auto value = std::make_pair(range, entry);
    int size;

    pthread_rwlock_wrlock(&clean_rtree_lock);
    clean_rtree_ptr->insert(value);
    size = clean_rtree_ptr->size();
    pthread_rwlock_unlock(&clean_rtree_lock);

    return size;
}

extern "C" int rtree_insert(uint64_t start, uint64_t end, long sn,
                            bool dirty, uint8_t *bytes)
{
    if (dirty)
    {
        return rtree_insert_dirty(start, end, sn, bytes);
    }
    else
    {
        return rtree_insert_clean(start, end, sn);
    }
}

extern "C" int rtree_remove(uint64_t start, uint64_t end, long sn, bool dirty)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = std::make_pair(block_range_entry(start, end, sn), byte_vector_t());
    auto value = std::make_pair(range, entry);
    rtree_t *rtree_ptr = nullptr;
    pthread_rwlock_t *lock_ptr = nullptr;

    if (dirty)
    {
        rtree_ptr = dirty_rtree_ptr;
        lock_ptr = &clean_rtree_lock;
    }
    else
    {
        rtree_ptr = clean_rtree_ptr;
        lock_ptr = &dirty_rtree_lock;
    }

    pthread_rwlock_wrlock(lock_ptr);
    rtree_ptr->remove(value);
    pthread_rwlock_unlock(lock_ptr);
    return rtree_ptr->size();
}

extern "C" uint64_t rtree_size(bool dirty)
{
    rtree_t *rtree_ptr = nullptr;

    if (dirty)
    {
        rtree_ptr = dirty_rtree_ptr;
    }
    else
    {
        rtree_ptr = clean_rtree_ptr;
    }

    return static_cast<uint64_t>(rtree_ptr->size());
}

extern "C" int rtree_query(block_range_entry_part **parts,
                           uint64_t start, uint64_t end)
{
    auto range = range_t(point_t(start), point_t(end));
    auto intersects = bgi::intersects(range);
    auto candidates = std::vector<value_t>();
    file_map_t file_map;

    pthread_rwlock_rdlock(&clean_rtree_lock);
    clean_rtree_ptr->query(intersects, std::back_inserter(candidates));
    pthread_rwlock_unlock(&clean_rtree_lock);

    // Insert results from the R-tree query into the interval map
    // XXX insert directly into the file_map?
    for (auto itr = candidates.begin(); itr != candidates.end(); ++itr)
    {
        uint64_t interval_start = itr->first.min_corner().get<0>();
        uint64_t interval_end = itr->first.max_corner().get<0>();
        auto addr_interval = addr_interval_t::closed(interval_start, interval_end);
        auto pair = std::make_pair(addr_interval, itr->second.first);

        file_map += pair;
    }

    // Allocate the return array
    *parts = static_cast<block_range_entry_part *>(malloc(sizeof(block_range_entry_part) * icl::interval_count(file_map)));
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

    return i;
}

extern "C" uint64_t rtree_dump(block_range_entry **entries)
{
    uint64_t n, i;

    pthread_rwlock_rdlock(&clean_rtree_lock);
    n = i = static_cast<uint64_t>(clean_rtree_ptr->size());
    *entries = static_cast<block_range_entry *>(malloc(sizeof(block_range_entry) * n));
    for (auto itr = clean_rtree_ptr->begin(); itr != clean_rtree_ptr->end(); ++itr)
    {
        (*entries)[--i] = itr->second.first;
    }
    pthread_rwlock_unlock(&clean_rtree_lock);
    return n;
}
