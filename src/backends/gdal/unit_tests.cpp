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

#define BOOST_TEST_MODULE Range Tree Tests
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
    rtree_insert("/tmp/a", 0, 1);
    BOOST_TEST(rtree_size() == 1);
    rtree_remove("/tmp/a", 0, 1);
    BOOST_TEST(rtree_size() == 0);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_size_test)
{
    const int max_results = 10;
    char *results[max_results];

    rtree_init();
    rtree_insert("/tmp/a", 0, 5);
    rtree_insert("/tmp/b", 4, 7);

    BOOST_TEST(rtree_query(results, max_results, 0, 3) == 1);
    free(results[0]);

    BOOST_TEST(rtree_query(results, max_results, 0, 4) == 2);
    free(results[0]);
    free(results[1]);

    BOOST_TEST(rtree_query(results, max_results, 4, 5) == 2);
    free(results[0]);
    free(results[1]);

    BOOST_TEST(rtree_query(results, max_results, 5, 7) == 2);
    free(results[0]);
    free(results[1]);

    BOOST_TEST(rtree_query(results, max_results, 6, 7) == 1);
    free(results[0]);

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_sort_test)
{
    const int max_results = 10;
    char *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert("/tmp/c", 8, 30);
    rtree_insert("/tmp/a", 0, 7);
    rtree_insert("/tmp/b", 7, 20);

    num_results = rtree_query(results, max_results, 8, 9);
    BOOST_TEST(num_results == 2);
    BOOST_TEST(strcmp(results[0], "/tmp/b") == 0);
    BOOST_TEST(strcmp(results[1], "/tmp/c") == 0);
    free(results[0]);
    free(results[1]);

    rtree_deinit();
}
