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

#define BOOST_TEST_MODULE Storage Tests
#include <boost/test/included/unit_test.hpp>

#include <vector>

#include <gdal.h>
#include <cpl_vsi.h>

#include "constants.h"
#include "storage.h"

constexpr uint64_t backed_extent_tag = 1 * EXTENT_SIZE;
constexpr uint64_t unbacked_extent_tag = 0 * EXTENT_SIZE;

void freshen_file()
{
    uint8_t *extent;
    char filename[0x100];

    // Create a file to work with
    sprintf(filename, EXTENT_TEMPLATE, "/vsimem", backed_extent_tag);
    VSILFILE *handle = VSIFOpenL(filename, "w");

    // Create an extent and store it in the file
    extent = new uint8_t[EXTENT_SIZE];
    memset(extent, 0xaa, EXTENT_SIZE);
    VSIFWriteL(extent, EXTENT_SIZE, 1, handle);
    delete extent;

    // Close the file
    VSIFCloseL(handle);
}

BOOST_AUTO_TEST_CASE(aligned_page_read_backed)
{
    uint8_t page[PAGE_SIZE] = {};
    uint64_t page_tag = backed_extent_tag + (42 * PAGE_SIZE);

    storage_init("/vsimem");
    freshen_file();

    memset(page, 0, PAGE_SIZE);
    aligned_page_read(page_tag + (0 * PAGE_SIZE), PAGE_SIZE, page);
    BOOST_TEST(page[0] == 0xaa);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0xaa);

    memset(page, 0, PAGE_SIZE);
    aligned_page_read(page_tag + (1 * PAGE_SIZE), PAGE_SIZE, page);
    BOOST_TEST(page[0] == 0xaa);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0xaa);

    memset(page, 0, PAGE_SIZE);
    aligned_page_read(page_tag + (2 * PAGE_SIZE), PAGE_SIZE / 2, page);
    BOOST_TEST(page[0] == 0xaa);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0x00);

    storage_deinit();
}

BOOST_AUTO_TEST_CASE(aligned_page_read_unbacked)
{
    uint8_t page[PAGE_SIZE] = {};
    uint64_t page_tag = unbacked_extent_tag + (42 * PAGE_SIZE);

    storage_init("/vsimem");
    freshen_file();

    aligned_page_read(page_tag, PAGE_SIZE, page);
    BOOST_TEST(page[0] == 0x33);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0x33);

    storage_deinit();
}

BOOST_AUTO_TEST_CASE(aligned_page_write_backed)
{
    uint8_t page[PAGE_SIZE] = {};
    uint64_t page_tag = backed_extent_tag + (107 * PAGE_SIZE);

    storage_init("/vsimem");
    freshen_file();

    memset(page, 0x01, PAGE_SIZE);
    aligned_whole_page_write(page_tag + (0 * PAGE_SIZE), page);
    aligned_page_read(page_tag + (0 * PAGE_SIZE), PAGE_SIZE, page);
    BOOST_TEST(page[0] == 0x01);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0x01);

    storage_deinit();
}

BOOST_AUTO_TEST_CASE(aligned_page_write_unbacked)
{
    uint8_t page[PAGE_SIZE] = {};
    uint64_t page_tag = 72 * EXTENT_SIZE;

    storage_init("/vsimem");
    freshen_file();

    memset(page, 0x01, PAGE_SIZE);
    aligned_whole_page_write(page_tag + (0 * PAGE_SIZE), page);
    aligned_page_read(page_tag + (0 * PAGE_SIZE), PAGE_SIZE, page);
    BOOST_TEST(page[0] == 0x01);
    BOOST_TEST(page[PAGE_SIZE - 1] == 0x01);

    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_read_unaligned_1)
{
    off_t offset = backed_extent_tag - 33;
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_read = storage_read(offset, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0x33);
    BOOST_TEST(bytes[bytes_read] == 0x55);

    delete bytes;
    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_read_unaligned_2)
{
    off_t offset = backed_extent_tag + 33;
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_read = storage_read(offset, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0xaa);
    BOOST_TEST(bytes[bytes_read] == 0x55);

    delete bytes;
    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_read_aligned)
{
    off_t offset = backed_extent_tag;
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_read = storage_read(offset, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0xaa);
    BOOST_TEST(bytes[bytes_read - 1] == 0xaa);
    BOOST_TEST(bytes[bytes_read] == 0x55);

    delete bytes;
    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_write_unaligned_1)
{
    off_t offset = backed_extent_tag - 33;
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;
    int bytes_written = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_written = storage_write(offset, size, bytes);
    BOOST_TEST(bytes_written == size);

    memset(bytes, 0, size + 1);
    bytes_read = storage_read(offset, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0x55);
    BOOST_TEST(bytes[bytes_read - 1] == 0x55);
    BOOST_TEST(bytes[bytes_read] == 0x00);

    delete bytes;
    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_write_unaligned_2)
{
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;
    int bytes_written = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_written = storage_write(backed_extent_tag + 33, size, bytes);
    BOOST_TEST(bytes_written == size);

    memset(bytes, 0, size + 1);
    bytes_read = storage_read(backed_extent_tag + 33, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0x55);
    BOOST_TEST(bytes[bytes_read - 1] == 0x55);
    BOOST_TEST(bytes[bytes_read] == 0x00);

    delete bytes;
    storage_deinit();
}

BOOST_AUTO_TEST_CASE(storage_write_aligned)
{
    off_t offset = backed_extent_tag;
    size_t size = 2 * PAGE_SIZE + 3;
    uint8_t *bytes = new uint8_t[size + 1];
    int bytes_read = 0;
    int bytes_written = 0;

    storage_init("/vsimem");
    freshen_file();

    memset(bytes, 0x55, size + 1);
    bytes_written = storage_write(offset, size, bytes);
    BOOST_TEST(bytes_written == size);

    memset(bytes, 0, size + 1);
    bytes_read = storage_read(offset, size, bytes);
    BOOST_TEST(bytes_read == size);
    BOOST_TEST(bytes[0] == 0x55);
    BOOST_TEST(bytes[bytes_read - 1] == 0x55);
    BOOST_TEST(bytes[bytes_read] == 0x00);

    delete bytes;
    storage_deinit();
}
