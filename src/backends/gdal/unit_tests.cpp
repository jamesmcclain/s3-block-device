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
