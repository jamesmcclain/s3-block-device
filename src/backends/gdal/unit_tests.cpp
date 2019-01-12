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

#define BOOST_TEST_MODULE R-tree Tests
#include <boost/test/included/unit_test.hpp>

#include "rtree.h"


BOOST_AUTO_TEST_CASE(rtree_init_test)
{
    BOOST_TEST(rtree_init() == 1);
}

BOOST_AUTO_TEST_CASE(rtree_deinit_test)
{
    rtree_init();
    BOOST_TEST(rtree_deinit() == 1);
}

BOOST_AUTO_TEST_CASE(rtree_insert_remove_test)
{
    rtree_init();
    BOOST_TEST(rtree_size() == 0);
    rtree_insert("/tmp/a", 0, 1, 0);
    BOOST_TEST(rtree_size() == 1);
    rtree_remove("/tmp/a", 0, 1, 0);
    BOOST_TEST(rtree_size() == 0);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_size_test)
{
    const int max_results = 10;
    struct file_interval *results[max_results];

    rtree_init();
    rtree_insert("/tmp/a", 0, 5, 0);
    rtree_insert("/tmp/b", 4, 7, 1);

    BOOST_TEST(rtree_query(results, max_results, 0, 3) == 1);
    for (int i = 0; i < 1; ++i) {
      free(results[i]->filename);
      free(results[i]);
    }

    BOOST_TEST(rtree_query(results, max_results, 0, 4) == 2);
    for (int i = 0; i < 2; ++i) {
      free(results[i]->filename);
      free(results[i]);
    }

    BOOST_TEST(rtree_query(results, max_results, 4, 5) == 2);
    for (int i = 0; i < 2; ++i) {
      free(results[i]->filename);
      free(results[i]);
    }

    BOOST_TEST(rtree_query(results, max_results, 5, 7) == 1);
    for (int i = 0; i < 1; ++i) {
      free(results[i]->filename);
      free(results[i]);
    }

    BOOST_TEST(rtree_query(results, max_results, 6, 7) == 1);
    for (int i = 0; i < 1; ++i) {
      free(results[i]->filename);
      free(results[i]);
    }

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_test_1)
{
    const int max_results = 10;
    struct file_interval *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert("/tmp/a", 0, 2, 0);
    rtree_insert("/tmp/b", 1, 3, 1);
    rtree_insert("/tmp/c", 2, -1, 2);

#pragma GCC diagnostic ignored "-Wwrite-strings"
    struct file_interval expected1[] = { {.filename = "/tmp/c", .start_closed = 1, .end_closed = 1, .start = 3, .end = 4} } ;
#pragma GCC diagnostic pop

    num_results = rtree_query(results, max_results, 3, 4);
    BOOST_TEST(num_results == 1);
    for (int i = 0; i < 1; ++i) {
        BOOST_TEST(results[i]->filename == expected1[i].filename);
        BOOST_TEST(results[i]->start_closed == expected1[i].start_closed);
        BOOST_TEST(results[i]->end_closed == expected1[i].end_closed);
        BOOST_TEST(results[i]->start == expected1[i].start);
        BOOST_TEST(results[i]->end == expected1[i].end);
        free(results[i]->filename);
        free(results[i]);
    }

#pragma GCC diagnostic ignored "-Wwrite-strings"
    struct file_interval expected2[] = { {.filename = "/tmp/a", .start_closed = 1, .end_closed = 0, .start = 0, .end = 1},
                                         {.filename = "/tmp/b", .start_closed = 1, .end_closed = 0, .start = 1, .end = 2},
                                         {.filename = "/tmp/c", .start_closed = 1, .end_closed = 1, .start = 2, .end = 3} } ;
#pragma GCC diagnostic pop

    num_results = rtree_query(results, max_results, 0, 3);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < 3; ++i) {
        BOOST_TEST(results[i]->filename == expected2[i].filename);
        BOOST_TEST(results[i]->start_closed == expected2[i].start_closed);
        BOOST_TEST(results[i]->end_closed == expected2[i].end_closed);
        BOOST_TEST(results[i]->start == expected2[i].start);
        BOOST_TEST(results[i]->end == expected2[i].end);
        free(results[i]->filename);
        free(results[i]);
    }

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_test_2)
{
    const int max_results = 10;
    struct file_interval *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert("/tmp/a", 0, 2, 0);
    rtree_insert("/tmp/b", 1, 3, 2);
    rtree_insert("/tmp/c", 2, -1, 1);

#pragma GCC diagnostic ignored "-Wwrite-strings"
    struct file_interval expected1[] = { {.filename = "/tmp/b", .start_closed = 1, .end_closed = 1, .start = 3, .end = 3},
                                         {.filename = "/tmp/c", .start_closed = 0, .end_closed = 1, .start = 3, .end = 4} } ;
#pragma GCC diagnostic pop

    num_results = rtree_query(results, max_results, 3, 4);
    BOOST_TEST(num_results == 2);
    for (int i = 0; i < 2; ++i) {
        BOOST_TEST(results[i]->filename == expected1[i].filename);
        BOOST_TEST(results[i]->start_closed == expected1[i].start_closed);
        BOOST_TEST(results[i]->end_closed == expected1[i].end_closed);
        BOOST_TEST(results[i]->start == expected1[i].start);
        BOOST_TEST(results[i]->end == expected1[i].end);
        free(results[i]->filename);
        free(results[i]);
    }

#pragma GCC diagnostic ignored "-Wwrite-strings"
    struct file_interval expected2[] = { {.filename = "/tmp/a", .start_closed = 1, .end_closed = 0, .start = 0, .end = 1},
                                         {.filename = "/tmp/b", .start_closed = 1, .end_closed = 1, .start = 1, .end = 3},
                                         {.filename = "/tmp/c", .start_closed = 0, .end_closed = 1, .start = 3, .end = 4} } ;
#pragma GCC diagnostic pop

    num_results = rtree_query(results, max_results, 0, 4);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < 3; ++i) {
        BOOST_TEST(results[i]->filename == expected2[i].filename);
        BOOST_TEST(results[i]->start_closed == expected2[i].start_closed);
        BOOST_TEST(results[i]->end_closed == expected2[i].end_closed);
        BOOST_TEST(results[i]->start == expected2[i].start);
        BOOST_TEST(results[i]->end == expected2[i].end);
        free(results[i]->filename);
        free(results[i]);
    }

#pragma GCC diagnostic ignored "-Wwrite-strings"
    struct file_interval expected3[] = { {.filename = "/tmp/a", .start_closed = 0, .end_closed = 0, .start = 1, .end = 1},
                                         {.filename = "/tmp/b", .start_closed = 1, .end_closed = 1, .start = 1, .end = 3},
                                         {.filename = "/tmp/c", .start_closed = 0, .end_closed = 0, .start = 3, .end = 3} } ;
#pragma GCC diagnostic pop

    num_results = rtree_query(results, max_results, 1, 3);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < 3; ++i) {
        BOOST_TEST(results[i]->filename == expected3[i].filename);
        BOOST_TEST(results[i]->start_closed == expected3[i].start_closed);
        BOOST_TEST(results[i]->end_closed == expected3[i].end_closed);
        BOOST_TEST(results[i]->start == expected3[i].start);
        BOOST_TEST(results[i]->end == expected3[i].end);
        free(results[i]->filename);
        free(results[i]);
    }

    rtree_deinit();
}
