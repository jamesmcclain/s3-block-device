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
    rtree_insert(0, 1, 0);
    BOOST_TEST(rtree_size() == 1);
    rtree_remove(0, 1, 0);
    BOOST_TEST(rtree_size() == 0);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_size_test)
{
    const int max_results = 10;
    block_range_entry_part *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert(0, 5, 0);
    rtree_insert(4, 7, 1);

    BOOST_TEST((num_results = rtree_query(results, max_results, 0, 3)) == 1);
    for (int i = 0; i < num_results; ++i) {
      free(results[i]);
    }

    BOOST_TEST((num_results = rtree_query(results, max_results, 0, 4)) == 2);
    for (int i = 0; i < num_results; ++i) {
      free(results[i]);
    }

    BOOST_TEST((num_results = rtree_query(results, max_results, 4, 5)) == 1);
    for (int i = 0; i < num_results; ++i) {
      free(results[i]);
    }

    BOOST_TEST((num_results = rtree_query(results, max_results, 5, 7)) == 1);
    for (int i = 0; i < num_results; ++i) {
      free(results[i]);
    }

    BOOST_TEST((num_results = rtree_query(results, max_results, 6, 7)) == 1);
    for (int i = 0; i < num_results; ++i) {
      free(results[i]);
    }

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_test_1)
{
    const int max_results = 10;
    struct block_range_entry_part *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert(0, 2, 0);
    rtree_insert(1, 3, 1);
    rtree_insert(2, -1, 2);

    struct block_range_entry_part expected1[] = { block_range_entry_part(block_range_entry(2, -1, 2), true, true, 3, 4) };

    num_results = rtree_query(results, max_results, 3, 4);
    BOOST_TEST(num_results == 1);
    for (int i = 0; i < num_results; ++i) {
        BOOST_TEST(*results[i] == expected1[i]);
        free(results[i]);
    }

    struct block_range_entry_part expected2[] = { block_range_entry_part(block_range_entry(0, 2, 0), true, false, 0, 1),
                                                  block_range_entry_part(block_range_entry(1, 3, 1), true, false, 1, 2),
                                                  block_range_entry_part(block_range_entry(2, -1, 2), true, true, 2, 3) };

    num_results = rtree_query(results, max_results, 0, 3);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < num_results; ++i) {
        BOOST_TEST(*results[i] == expected2[i]);
        free(results[i]);
    }

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_test_2)
{
    const int max_results = 10;
    struct block_range_entry_part *results[max_results];
    int num_results;

    rtree_init();
    rtree_insert(0, 2, 0);
    rtree_insert(1, 3, 2);
    rtree_insert(2, -1, 1);

    struct block_range_entry_part expected1[] = { block_range_entry_part(block_range_entry(1, 3, 2), true, true, 3, 3),
                                                  block_range_entry_part(block_range_entry(2, -1, 1), false, true, 3, 4) };

    num_results = rtree_query(results, max_results, 3, 4);
    BOOST_TEST(num_results == 2);
    for (int i = 0; i < num_results; ++i) {
        BOOST_TEST(*results[i] == expected1[i]);
        free(results[i]);
    }

    struct block_range_entry_part expected2[] = { block_range_entry_part(block_range_entry(0, 2,0), true, false, 0, 1),
                                                  block_range_entry_part(block_range_entry(1, 3, 2), true, true, 1, 3),
                                                  block_range_entry_part(block_range_entry(2, -1, 1), false, true, 3, 4) };

    num_results = rtree_query(results, max_results, 0, 4);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < num_results; ++i) {
        BOOST_TEST(*results[i] == expected2[i]);
        free(results[i]);
    }

    struct block_range_entry_part expected3[] = { block_range_entry_part(block_range_entry(1, 3, 2), true, true, 1, 3) };

    num_results = rtree_query(results, max_results, 1, 3);
    BOOST_TEST(num_results == 1);
    for (int i = 0; i < num_results; ++i) {
        BOOST_TEST(*results[i] == expected3[i]);
        free(results[i]);
    }

    rtree_deinit();
}
