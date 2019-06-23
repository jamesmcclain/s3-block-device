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
#include <cstdint>

#include <algorithm>
#include <cassert>
#include <map>

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include "storage.h"

struct disk_page
{
    uint8_t bytes[PAGE_SIZE]; // XXX maybe a pointer

    disk_page() : bytes{} {};
    disk_page(const uint8_t *);
    disk_page &operator=(disk_page &&rhs) = default;
};

disk_page::disk_page(const uint8_t *bytes)
{
    memcpy(this->bytes, bytes, PAGE_SIZE);
}

struct disk_extent
{
    bool dirty;
    pthread_rwlock_t lock;
    uint8_t bytes[EXTENT_SIZE]; // XXX maybe a pointer

    disk_extent() : dirty(true), lock(PTHREAD_RWLOCK_INITIALIZER), bytes{} {};
    disk_extent(uint64_t extent_tag, VSILFILE *handle);
    disk_extent &operator=(disk_extent &&rhs) = default;
};

disk_extent::disk_extent(uint64_t extent_tag, VSILFILE *handle) : dirty(false), lock(PTHREAD_RWLOCK_INITIALIZER)
{
    if (handle != NULL)
    {
        if (VSIFReadL(this->bytes, EXTENT_SIZE, 1, handle) != 1)
        {
            fprintf(stderr, "%016lX openable but not readable\n", extent_tag);
            memset(this->bytes, 0, EXTENT_SIZE);
        }
    }
    else if (handle == NULL)
    {
        memset(this->bytes, 0, EXTENT_SIZE);
    }
}

typedef std::map<uint64_t, struct disk_page> page_map_t;
typedef std::map<uint64_t, struct disk_extent> extent_map_t;

static page_map_t *page_map = nullptr;
static pthread_rwlock_t page_lock = PTHREAD_RWLOCK_INITIALIZER;

static extent_map_t *extent_map = nullptr;
static pthread_rwlock_t extent_lock = PTHREAD_RWLOCK_INITIALIZER;

static const char *blockdir = nullptr;

/**
 * Initialize storage.
 *
 * @param _blockdir A pointer to a string giving the path to the storage directory
 */
extern "C" void storage_init(const char *_blockdir)
{
    assert(_blockdir != nullptr);
    blockdir = _blockdir;

    if (page_map == nullptr)
    {
        page_map = new page_map_t();
    }
    if (extent_map == nullptr)
    {
        extent_map = new extent_map_t();
    }
    if (page_map == nullptr || extent_map == nullptr)
    {
        throw std::bad_alloc();
    }
}

/**
 * Deinitialize storage.
 */
extern "C" void storage_deinit()
{
    if (page_map != nullptr)
    {
        delete page_map;
        page_map = nullptr;
    }
    if (extent_map != nullptr)
    {
        delete extent_map;
        extent_map = nullptr;
    }
}

const void *debug_extent_address(uint64_t extent_tag)
{
    const auto &e = extent_map->operator[](extent_tag);
    return e.bytes;
}

/**
 * Read an extent from external storage.
 *
 * @param extent_tag The tag of the extent to read from storage
 * @return Boolean indicating success or failure
 */
bool extent_read(uint64_t extent_tag)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK))); // Assert alignment to extent size

    auto count = extent_map->count(extent_tag);
    assert((0 <= count) && (count < 2));

    if (count == 1) // Extent already exists
    {
        return true;
    }
    else // if (count == 0) // Extent needs to be read
    {
        char filename[0x100];
        VSILFILE *handle = NULL;

        sprintf(filename, "%s/%016lX", blockdir, extent_tag);

        handle = VSIFOpenL(filename, "r");
        extent_map->operator[](extent_tag) = std::move(disk_extent(extent_tag, handle));
        if (handle != NULL)
        {
            VSIFCloseL(handle);
        }
        return true;
    }
}

/**
 * Attempt to read and return a page or less worth of data.
 *
 * @param page_tag The tag of the page to read from
 * @param size The number of bytes to read (must be <= the size of a page)
 * @param bytes The array in which to return the bytes
 * @return Boolean indicating success or failure
 */
bool aligned_page_read(uint64_t page_tag, uint16_t size, uint8_t *bytes)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment
    assert(size <= PAGE_SIZE);                     // Assert request size <= page size

    pthread_rwlock_rdlock(&page_lock);

    auto count = page_map->count(page_tag);
    assert((0 <= count) && (count < 2));

    if (count == 0)
    {
        auto extent_tag = page_tag & (~EXTENT_MASK);
        auto extent_offset = page_tag & EXTENT_MASK;

        pthread_rwlock_wrlock(&extent_lock);
        if (extent_read(extent_tag) != true)
        {
            return false;
        }

        auto extent_itr = extent_map->find(extent_tag);
        if (extent_itr == extent_map->end())
        {
            pthread_rwlock_unlock(&extent_lock);
            pthread_rwlock_unlock(&page_lock);
            return false;
        }
        else
        {
            pthread_rwlock_rdlock(&(extent_itr->second.lock));
            memcpy(bytes, extent_itr->second.bytes + extent_offset, size);
            pthread_rwlock_unlock(&(extent_itr->second.lock));
            pthread_rwlock_unlock(&extent_lock);
            pthread_rwlock_unlock(&page_lock);
            return true;
        }
    }
    else //if (count == 1)
    {
        const auto itr = page_map->find(page_tag);
        if (itr == page_map->end())
        {
            pthread_rwlock_unlock(&page_lock);
            return false;
        }
        else
        {
            memcpy(bytes, itr->second.bytes, size);
            pthread_rwlock_unlock(&page_lock);
            return true;
        }
    }
}

/**
 * Attempt to write a page or less worth of data.
 *
 * @param page_tag The tag of the page to write from
 * @param size The number of bytes to write (must be <= the size of a page)
 * @param bytes The array from which to write the bytes
 * @return Boolean indicating success or failure
 */
bool aligned_page_write(uint64_t page_tag, uint16_t size, const uint8_t *bytes)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment
    assert(size <= PAGE_SIZE);                     // Assert request size <= page size

    if (size == PAGE_SIZE)
    {
        pthread_rwlock_wrlock(&page_lock);
        page_map->operator[](page_tag) = std::move(disk_page(bytes));
        pthread_rwlock_unlock(&page_lock);
        return true;
    }
    else //if (size < PAGE_SIZE)
    {
        pthread_rwlock_wrlock(&page_lock);
        auto count = page_map->count(page_tag);
        assert((0 <= count) && (count < 2));

        if (count == 0)
        {
            auto extent_tag = page_tag & (~EXTENT_MASK);
            auto extent_offset = page_tag & EXTENT_MASK;

            pthread_rwlock_wrlock(&extent_lock);
            if (extent_read(extent_tag) != true)
            {
                pthread_rwlock_unlock(&extent_lock);
                pthread_rwlock_unlock(&page_lock);
                return false;
            }

            auto extent_itr = extent_map->find(extent_tag);
            if (extent_itr == extent_map->end())
            {
                pthread_rwlock_unlock(&extent_lock);
                pthread_rwlock_unlock(&page_lock);
                return false;
            }
            else
            {
                pthread_rwlock_rdlock(&(extent_itr->second.lock));
                auto &pg = page_map->operator[](page_tag) = std::move(disk_page());
                memcpy(pg.bytes, extent_itr->second.bytes + extent_offset, PAGE_SIZE);
                memcpy(pg.bytes, bytes, size);
                pthread_rwlock_unlock(&(extent_itr->second.lock));
                pthread_rwlock_unlock(&extent_lock);
                pthread_rwlock_unlock(&page_lock);
                return true;
            }
        }
        else //if (count == 1)
        {
            auto page_itr = page_map->find(page_tag);
            if (page_itr == page_map->end())
            {
                pthread_rwlock_unlock(&page_lock);
                return false;
            }
            else
            {
                memcpy(page_itr->second.bytes, bytes, size);
                pthread_rwlock_unlock(&page_lock);
                return true;
            }
        }
    }
}

/**
 * Read bytes from storage.  Aligned reads are preferred.
 *
 * @param offset The virtual block device offset to read from
 * @param size The number of bytes to read
 * @param bytes The buffer to read bytes into
 * @return The number of bytes read or a negative errno
 */
extern "C" int storage_read(off_t offset, size_t size, uint8_t *bytes)
{
    uint64_t page_tag = offset & (~PAGE_MASK);

    if (static_cast<uint64_t>(offset) == page_tag) // if aligned
    {
        int bytes_read = 0;
        while (size > 0)
        {
            uint16_t size16 = size <= PAGE_SIZE ? static_cast<uint16_t>(size) : static_cast<uint16_t>(PAGE_SIZE);
            if (aligned_page_read(page_tag, size16, bytes) == true)
            {
                offset += size16;
                size -= size16;
                bytes += size16;
                bytes_read += size16;
            }
            else if (bytes_read == 0)
            {
                return -EIO;
            }
            else
            {
                return bytes_read;
            }
        }
        return bytes_read;
    }
    else // if not aligned
    {
        uint8_t page[PAGE_SIZE];
        auto diff = offset - page_tag;
        assert(diff < PAGE_SIZE);
        auto left_in_page = PAGE_SIZE - diff;
        uint16_t wanted = static_cast<uint16_t>(std::min(left_in_page, size));

        if (aligned_page_read(page_tag, PAGE_SIZE, page) != true)
        {
            // If unable to read, report IO error
            return -EIO;
        }
        else
        {
            // Otherwise report number of bytes read
            memcpy(bytes, page + diff, wanted);
            return static_cast<int>(wanted);
        }
    }
}

/**
 * Write bytes to storage.  Aligned writes are prefferred.
 *
 * @param offset The virtual block device offset to write to
 * @param size The number of bytes to write
 * @param bytes The buffer to read bytes from
 * @return The number of bytes written or a negative errno
 */
extern "C" int storage_write(off_t offset, size_t size, const uint8_t *bytes)
{
    uint64_t page_tag = offset & (~PAGE_MASK);

    if (static_cast<uint64_t>(offset) == page_tag) // if aligned
    {
        int bytes_written = 0;
        while (size > 0)
        {
            uint16_t size16 = size <= PAGE_SIZE ? static_cast<uint16_t>(size) : static_cast<uint16_t>(PAGE_SIZE);
            if (aligned_page_write(page_tag, size16, bytes) == true)
            {
                offset += size16;
                size -= size16;
                bytes += size16;
                bytes_written += size16;
            }
            else if (bytes_written == 0)
            {
                return -EIO;
            }
            else
            {
                return bytes_written;
            }
        }
        return bytes_written;
    }
    else // if not aligned
    {
        uint8_t page[PAGE_SIZE] = {};
        auto diff = offset - page_tag;
        assert(diff < PAGE_SIZE);
        auto left_in_page = PAGE_SIZE - diff;
        uint16_t wanted = static_cast<uint16_t>(std::min(left_in_page, size));

        aligned_page_read(page_tag, PAGE_SIZE, page); // XXX in a race
        memcpy(page + diff, bytes, wanted);
        if (aligned_page_write(page_tag, PAGE_SIZE, page) != true)
        {
            // If not able to write, report IO error
            return -EIO;
        }
        else
        {
            // Otherwise, report number of byte written
            return static_cast<int>(wanted);
        }
    }
}
