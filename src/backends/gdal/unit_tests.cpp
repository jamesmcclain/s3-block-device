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

#define BOOST_TEST_MODULE R - tree Tests
#include <boost/test/included/unit_test.hpp>

#include <vector>

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

BOOST_AUTO_TEST_CASE(rtree_insert_remove_storage_test)
{
    bool in_memory = false;

    rtree_init();
    BOOST_TEST(rtree_size(in_memory) == 0u);
    BOOST_TEST(rtree_insert(0, 1, 0, in_memory, nullptr) == 1);
    BOOST_TEST(rtree_size(in_memory) == 1u);
    BOOST_TEST(rtree_remove(0, 1, 0) == 0);
    BOOST_TEST(rtree_size(in_memory) == 0u);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_memory_range_merge_test)
{
    bool in_memory = true;

    rtree_init();
    BOOST_TEST(rtree_insert(3, 3, 0, in_memory, nullptr) == 1);
    BOOST_TEST(rtree_insert(3, 3, 1, in_memory, nullptr) == 1);
    BOOST_TEST(rtree_size(in_memory) == 1u);
    BOOST_TEST(rtree_insert(5, 5, 2, in_memory, nullptr) == 2);
    BOOST_TEST(rtree_size(in_memory) == 2u);
    BOOST_TEST(rtree_insert(4, 4, 3, in_memory, nullptr) == 1);
    BOOST_TEST(rtree_size(in_memory) == 1u);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_memory_contents_merge_test)
{
    uint8_t zeros[] = {0, 0, 0};
    uint8_t ones[] = {1, 1, 1};
    uint8_t actual[7];
    auto expected = std::vector<uint8_t>{0, 0, 1, 1, 1, 0, 0};
    struct block_range_entry_part *results;

    rtree_init();
    BOOST_TEST(rtree_insert(2, 4, 0, true, zeros) == 1);
    BOOST_TEST(rtree_insert(6, 8, 1, true, zeros) == 2);
    BOOST_TEST(rtree_insert(4, 6, 2, true, ones) == 1);
    rtree_query(2, 8, actual, &results);
    free(results);
    BOOST_TEST(std::vector<uint8_t>(actual, actual + 7) == expected);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_memory_adjacent_merge_test)
{
    uint8_t zeros[] = {0, 0, 0};
    uint8_t ones[] = {1, 1, 1};
    uint8_t actual[6];
    auto expected = std::vector<uint8_t>{1, 1, 1, 0, 0, 0};
    struct block_range_entry_part *results;

    rtree_init();
    BOOST_TEST(rtree_insert(2, 4, 0, true, ones) == 1);
    BOOST_TEST(rtree_insert(5, 7, 1, true, zeros) == 1);
    rtree_query(2, 6, actual, &results);
    free(results);
    BOOST_TEST(std::vector<uint8_t>(actual, actual + 6) == expected);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_memory_storage_query_test)
{
    struct block_range_entry_part *results;
    struct block_range_entry_part expected[] = {
        block_range_entry_part(block_range_entry(0, 2, 0), 0, 0),
        block_range_entry_part(block_range_entry(6, 9, 2), 8, 9)};

    rtree_init();
    rtree_insert(0, 2, 0, false, nullptr);
    rtree_insert(3, 5, 1, false, nullptr);
    rtree_insert(6, 9, 2, false, nullptr);
    rtree_insert(1, 7, 3, true, nullptr);
    BOOST_TEST(rtree_query(0, 9, nullptr, &results) == 2);
    for (int i = 0; i < 2; ++i)
    {
        BOOST_TEST(results[i] == expected[i]);
    }
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(ext2_problem_test)
{
    const int N = 4096;
    uint8_t buffer[N];
    uint64_t sum = 0;
    struct block_range_entry_part *results;

    memset(buffer, 0, N);
    rtree_init();
    rtree_insert(1024, N - 1, 0, true, nullptr);
    buffer[0] = 1;
    buffer[1024] = 1;
    rtree_query(0, N - 1, buffer, &results);
    free(results);
    for (int i = 0; i < N; ++i)
    {
        sum += buffer[i];
    }
    BOOST_TEST(sum == 0u);
    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_size_storage_test)
{
    bool in_memory = false;
    struct block_range_entry_part *results;
    int num_results;

    rtree_init();
    rtree_insert(0, 5, 0, in_memory, nullptr);
    rtree_insert(4, 7, 1, in_memory, nullptr);

    BOOST_TEST((num_results = rtree_query(0, 3, nullptr, &results)) == 1);
    free(results);

    BOOST_TEST((num_results = rtree_query(0, 4, nullptr, &results)) == 2);
    free(results);

    BOOST_TEST((num_results = rtree_query(4, 5, nullptr, &results)) == 1);
    free(results);

    BOOST_TEST((num_results = rtree_query(5, 7, nullptr, &results)) == 1);
    free(results);

    BOOST_TEST((num_results = rtree_query(6, 7, nullptr, &results)) == 1);
    free(results);

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_storage_test_1)
{
    bool in_memory = false;
    struct block_range_entry_part *results;
    int num_results;

    rtree_init();
    rtree_insert(0, 2, 0, in_memory, nullptr);
    rtree_insert(1, 3, 1, in_memory, nullptr);
    rtree_insert(2, -1, 2, in_memory, nullptr);

    struct block_range_entry_part expected1[] = {
        block_range_entry_part(block_range_entry(2, -1, 2), 3, 4)};

    num_results = rtree_query(3, 4, nullptr, &results);
    BOOST_TEST(num_results == 1);
    for (int i = 0; i < num_results; ++i)
    {
        BOOST_TEST(results[i] == expected1[i]);
    }
    free(results);

    struct block_range_entry_part expected2[] = {
        block_range_entry_part(block_range_entry(0, 2, 0), 0, 0),
        block_range_entry_part(block_range_entry(1, 3, 1), 1, 1),
        block_range_entry_part(block_range_entry(2, -1, 2), 2, 3)};

    num_results = rtree_query(0, 3, nullptr, &results);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < num_results; ++i)
    {
        BOOST_TEST(results[i] == expected2[i]);
    }
    free(results);

    rtree_deinit();
}

BOOST_AUTO_TEST_CASE(rtree_query_result_interval_storage_test_2)
{
    bool in_memory = false;
    struct block_range_entry_part *results;
    int num_results;

    rtree_init();
    rtree_insert(0, 2, 0, in_memory, nullptr);
    rtree_insert(1, 3, 2, in_memory, nullptr);
    rtree_insert(2, -1, 1, in_memory, nullptr);

    struct block_range_entry_part expected1[] = {
        block_range_entry_part(block_range_entry(1, 3, 2), 3, 3),
        block_range_entry_part(block_range_entry(2, -1, 1), 4, 4)};

    num_results = rtree_query(3, 4, nullptr, &results);
    BOOST_TEST(num_results == 2);
    for (int i = 0; i < num_results; ++i)
    {
        BOOST_TEST(results[i] == expected1[i]);
    }
    free(results);

    struct block_range_entry_part expected2[] = {
        block_range_entry_part(block_range_entry(0, 2, 0), 0, 0),
        block_range_entry_part(block_range_entry(1, 3, 2), 1, 3),
        block_range_entry_part(block_range_entry(2, -1, 1), 4, 4)};

    num_results = rtree_query(0, 4, nullptr, &results);
    BOOST_TEST(num_results == 3);
    for (int i = 0; i < num_results; ++i)
    {
        BOOST_TEST(results[i] == expected2[i]);
    }
    free(results);

    struct block_range_entry_part expected3[] = {
        block_range_entry_part(block_range_entry(1, 3, 2), 1, 3)};

    num_results = rtree_query(1, 3, nullptr, &results);
    BOOST_TEST(num_results == 1);
    for (int i = 0; i < num_results; ++i)
    {
        BOOST_TEST(results[i] == expected3[i]);
    }
    free(results);

    rtree_deinit();
}
