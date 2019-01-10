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

// https://www.boost.org/doc/libs/1_69_0/libs/geometry/doc/html/geometry/spatial_indexes/rtree_examples/index_stored_in_mapped_file_using_boost_interprocess.html
// https://www.boost.org/doc/libs/1_69_0/libs/geometry/doc/html/geometry/spatial_indexes/rtree_examples.html
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>

#include "rtree.h"


namespace bi  = boost::interprocess;
namespace bg  = boost::geometry;
namespace bgm = boost::geometry::model;
namespace bgi = boost::geometry::index;

typedef bgm::point<uint64_t, 1, bg::cs::cartesian> point_t;
typedef bgm::box<point_t> range_t;
typedef std::pair<range_t, std::string> value_t;
typedef bgi::linear<16, 4> params_t;
typedef bgi::indexable<value_t> indexable_t;
typedef bgi::rtree<value_t, params_t, indexable_t> rtree_t;


rtree_t * rtree_ptr = nullptr;


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

extern "C" int rtree_insert(const char *filename, uint64_t start, uint64_t end)
{
    auto range = range_t(point_t(start), point_t(end));
    auto filename_str = std::string(strdup(filename));
    auto value = std::make_pair(range, filename_str);

    rtree_ptr->insert(value);
    return 1;
}

extern "C" int rtree_query(char **filenames, uint64_t start, uint64_t end)
{
    auto range = range_t(point_t(start), point_t(end));
    auto intersects = bgi::intersects(range);
    auto candidates = std::vector < value_t > ();

    rtree_ptr->query(intersects, std::back_inserter(candidates));

    return 1;
}
