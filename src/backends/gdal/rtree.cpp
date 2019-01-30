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
#include <boost/icl/split_interval_map.hpp>

#include "rtree.h"
#include "block_range_entry.h"

// R-tree
namespace bi = boost::interprocess;
namespace bg = boost::geometry;
namespace bgm = boost::geometry::model;
namespace bgi = boost::geometry::index;
typedef bgm::point<uint64_t, 1, bg::cs::cartesian> point_t;
typedef bgm::box<point_t> range_t;
typedef std::pair<range_t, block_range_entry> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;

// Intervals
namespace icl = boost::icl;
typedef icl::interval_map<uint64_t, block_range_entry> file_map_t;
typedef icl::interval<uint64_t> addr_interval_t;

static rtree_t *rtree_ptr = nullptr;
static pthread_rwlock_t rtree_lock = PTHREAD_RWLOCK_INITIALIZER;

extern "C" int rtree_init()
{
    if (rtree_ptr == nullptr)
        rtree_ptr = new rtree_t();
    return 1;
}

extern "C" int rtree_deinit()
{
    if (rtree_ptr != nullptr)
        delete rtree_ptr;
    rtree_ptr = nullptr;
    return 1;
}

extern "C" int rtree_insert(uint64_t start, uint64_t end, long sn)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = block_range_entry(start, end, sn);
    auto value = std::make_pair(range, entry);

    pthread_rwlock_wrlock(&rtree_lock);
    rtree_ptr->insert(value);
    pthread_rwlock_unlock(&rtree_lock);
    return rtree_ptr->size();
}

extern "C" int rtree_remove(uint64_t start, uint64_t end, long sn)
{
    auto range = range_t(point_t(start), point_t(end));
    auto entry = block_range_entry(start, end, sn);
    auto value = std::make_pair(range, entry);

    pthread_rwlock_wrlock(&rtree_lock);
    rtree_ptr->remove(value);
    pthread_rwlock_unlock(&rtree_lock);
    return rtree_ptr->size();
}

extern "C" uint64_t rtree_size()
{
    return static_cast<uint64_t>(rtree_ptr->size());
}

extern "C" int rtree_query(block_range_entry_part **parts, uint64_t start, uint64_t end)
{
    auto range = range_t(point_t(start), point_t(end));
    auto intersects = bgi::intersects(range);
    auto candidates = std::vector<value_t>();
    file_map_t file_map;

    pthread_rwlock_rdlock(&rtree_lock);
    rtree_ptr->query(intersects, std::back_inserter(candidates));
    pthread_rwlock_unlock(&rtree_lock);

    // Insert results from the R-tree query into the interval map
    for (auto itr = candidates.begin(); itr != candidates.end(); ++itr)
    {
        uint64_t interval_start = itr->first.min_corner().get<0>();
        uint64_t interval_end = itr->first.max_corner().get<0>();
        auto addr_interval = addr_interval_t::closed(interval_start, interval_end);
        auto pair = std::make_pair(addr_interval, itr->second);

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

    pthread_rwlock_rdlock(&rtree_lock);
    n = i = static_cast<uint64_t>(rtree_ptr->size());
    *entries = static_cast<block_range_entry *>(malloc(sizeof(block_range_entry) * n));
    for (auto itr = rtree_ptr->begin(); itr != rtree_ptr->end(); ++itr)
    {
        (*entries)[--i] = itr->second;
    }
    pthread_rwlock_unlock(&rtree_lock);
    return n;
}
