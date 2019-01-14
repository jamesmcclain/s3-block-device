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
#include "timed_string.hpp"


// R-tree
namespace bi = boost::interprocess;
namespace bg = boost::geometry;
namespace bgm = boost::geometry::model;
namespace bgi = boost::geometry::index;
typedef bgm::point<uint64_t, 1, bg::cs::cartesian> point_t;
typedef bgm::box<point_t> range_t;
typedef std::pair<range_t, timed_string_t> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;

// Intervals
namespace icl = boost::icl;
typedef icl::interval_map<uint64_t, timed_string_t> file_map_t;
typedef icl::interval<uint64_t> addr_interval_t;

rtree_t *rtree_ptr = nullptr;


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

extern "C" int rtree_insert(const char *filename, uint64_t start, uint64_t end, long nanos)
{
    auto range = range_t(point_t(start), point_t(end));
    auto filename_str = std::string(filename);
    auto value = std::make_pair(range, timed_string_t(nanos, filename_str));

    rtree_ptr->insert(value);
    return rtree_ptr->size();
}

extern "C" int rtree_remove(const char *filename, uint64_t start, uint64_t end, long nanos)
{
    auto range = range_t(point_t(start), point_t(end));
    auto filename_str = std::string(filename);
    auto value = std::make_pair(range, timed_string_t(nanos, filename_str));

    rtree_ptr->remove(value);
    return rtree_ptr->size();
}

extern "C" int rtree_size()
{
    return rtree_ptr->size();
}

extern "C" int rtree_query(struct file_interval **file_intervals, int max_results,
                           uint64_t start, uint64_t end)
{
    auto range = range_t(point_t(start), point_t(end));
    auto intersects = bgi::intersects(range);
    auto candidates = std::vector<value_t>();
    file_map_t file_map;

    rtree_ptr->query(intersects, std::back_inserter(candidates));

    int i = 0;
    if (file_intervals != nullptr) {

        for (auto itr = candidates.begin(); (itr != candidates.end()) && (i < max_results); ++itr) {
            uint64_t addr_start = itr->first.min_corner().get<0>();
            uint64_t addr_end = itr->first.max_corner().get<0>();
            auto addr_interval = addr_interval_t::closed(addr_start, addr_end);
            auto pair = std::make_pair(addr_interval, itr->second);

            file_map += pair;
        }

        for (auto itr = file_map.begin(); itr != file_map.end(); ++itr) {
            struct file_interval *file_interval = nullptr;
            uint64_t addr_start = std::max(itr->first.lower(), start);
            uint64_t addr_end = std::min(itr->first.upper(), end);
            auto interval = itr->first;
            auto timed_interval = itr->second;

            if (addr_start <= addr_end) {

                file_interval =
                    static_cast<struct file_interval *>(malloc(sizeof(struct file_interval)));
                if (file_interval == nullptr) {
                    exit(-1);
                }

                file_interval->start = addr_start;
                file_interval->end = addr_end;
                file_interval->start_closed = icl::contains(interval, addr_start);
                file_interval->end_closed = icl::contains(interval, addr_end);
                if ((addr_end - addr_start < 2) && !file_interval->start_closed
                    && !file_interval->end_closed) {
                    free(file_interval);
                } else {
                    file_interval->filename = strdup(timed_interval.filename().c_str());
                    file_intervals[i++] = file_interval;
                }
            }
        }
    }

    return i;
}
