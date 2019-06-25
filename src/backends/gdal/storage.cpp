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

#if 0
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#endif

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include "storage.h"

struct disk_page
{
    uint8_t *bytes;
    bool dirty;

    // constructor
    disk_page() : bytes(new uint8_t[PAGE_SIZE]), dirty(false)
    {
        memset(bytes, 0, PAGE_SIZE);
    }

    // copy constructor
    disk_page(const disk_page &rhs) : bytes(new uint8_t[PAGE_SIZE]), dirty(rhs.dirty)
    {
        memcpy(bytes, rhs.bytes, PAGE_SIZE);
    }

    // move constructor
    disk_page(disk_page &&rhs) noexcept : bytes(rhs.bytes), dirty(rhs.dirty)
    {
        rhs.bytes = nullptr;
    }

    // bytes constructor
    disk_page(const uint8_t *bytes) : bytes(new uint8_t[PAGE_SIZE]), dirty(false)
    {
        memcpy(this->bytes, bytes, PAGE_SIZE);
    }

    // destructor
    ~disk_page()
    {
        if (bytes != nullptr)
        {
            delete bytes;
            bytes = nullptr;
        }
    }

    // copy assignment
    disk_page &operator=(const disk_page &rhs)
    {
        memcpy(bytes, rhs.bytes, PAGE_SIZE);
        dirty = rhs.dirty;
        return *this;
    }

    // move assignment
    disk_page &operator=(disk_page &&rhs) noexcept
    {
        bytes = rhs.bytes;
        rhs.bytes = nullptr;
        dirty = rhs.dirty;
        return *this;
    }
};

typedef std::map<uint64_t, struct disk_page> page_map_t;
typedef std::map<uint64_t, struct disk_extent> extent_map_t;

static page_map_t *page_map = nullptr;
static pthread_rwlock_t page_lock = PTHREAD_RWLOCK_INITIALIZER;

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
    if (page_map == nullptr)
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
}

/**
 * Flush a page and all of its extent-mates to storage.  Only one
 * instance of this function should ever be active at once.
 */
bool flush_page_and_friends(uint64_t page_tag) // AAA confirmed working in the sense that the sequence of writes is reproduced on disk
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    uint8_t *extent = new uint8_t[EXTENT_SIZE];

    uint64_t extent_tag = (page_tag & (~EXTENT_MASK));

    // Bring all pages in
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t inner_offset = i * PAGE_SIZE;
        aligned_page_read(extent_tag + inner_offset, PAGE_SIZE, extent + inner_offset);
    }

#if 0
    if (extent_tag == 0x400000)
    {
        int fd = open("/tmp/extent.mp3", O_RDWR);
        write(fd, extent, EXTENT_SIZE);
        close(fd);
    }
#endif

    // Open extent file
    char filename[0x100];
    VSILFILE *handle = NULL;
    sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
    if ((handle = VSIFOpenL(filename, "w")) == NULL)
    {
        delete extent;
        return false;
    }

    // Copy all pages out and remove them from page_map
    pthread_rwlock_wrlock(&page_lock);
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t inner_offset = i * PAGE_SIZE;
        uint64_t inner_page_tag = extent_tag + inner_offset;
        while (VSIFWriteL(extent + inner_offset, PAGE_SIZE, 1, handle) != 1) // XXX
        {
        }
        page_map->erase(inner_page_tag);
    }
    VSIFFlushL(handle);
    VSIFCloseL(handle);
    pthread_rwlock_unlock(&page_lock);

    delete extent;
    return true;
}

void storage_flush()
{
    while (true)
    {
        pthread_rwlock_rdlock(&page_lock);
        auto itr = page_map->begin();
        if (itr == page_map->end())
        {
            pthread_rwlock_unlock(&page_lock);
            return;
        }
        else
        {
            uint64_t page_tag = itr->first;
            pthread_rwlock_unlock(&page_lock);
            flush_page_and_friends(page_tag);
        }
    }
}

#define LOCK_AND_GET_PAGE(fn)                              \
    fn(&page_lock);                                        \
    if (page_map->count(page_tag) > 0)                     \
    {                                                      \
        const auto &page = page_map->operator[](page_tag); \
        memcpy(bytes, page.bytes, size);                   \
        pthread_rwlock_unlock(&page_lock);                 \
        return true;                                       \
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

    LOCK_AND_GET_PAGE(pthread_rwlock_rdlock);

    pthread_rwlock_unlock(&page_lock);

    LOCK_AND_GET_PAGE(pthread_rwlock_wrlock);

    uint64_t extent_tag = (page_tag & (~EXTENT_MASK));
    char filename[0x100];
    VSILFILE *handle = NULL;

    // Attempt to open extent file and attempt to load pages.  If not
    // able to do so, create an empy page.
    sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
    handle = VSIFOpenL(filename, "r");
    if (handle != NULL) // XXX distinguish between nonexistent and unopenable
    {
        for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
        {
            uint64_t inner_offset = (i * PAGE_SIZE);
            uint64_t inner_page_tag = extent_tag + inner_offset;

            if (page_map->count(inner_page_tag) < 1)
            {
                while (VSIFSeekL(handle, inner_offset, SEEK_SET) != 0) // XXX
                {
                }
                while (VSIFReadL(page_map->operator[](inner_page_tag).bytes, PAGE_SIZE, 1, handle) != 1) // XXX
                {
                }
            }
        }
        VSIFCloseL(handle);
    }

    const auto &page = page_map->operator[](page_tag);
    memcpy(bytes, page.bytes, size);

    pthread_rwlock_unlock(&page_lock);

    return true;
}

/**
 * Attempt to write a page or less worth of data.
 *
 * @param page_tag The tag of the page to write from
 * @param size The number of bytes to write (must be <= the size of a page)
 * @param bytes The array from which to write the bytes
 * @return Boolean indicating success or failure
 */
bool aligned_page_write(uint64_t page_tag, const uint8_t *bytes)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    uint64_t extent_tag = (page_tag & (~EXTENT_MASK));
#if 0
    if (extent_tag == 0x400000)
    {
        int fd = open("/tmp/write.mp3", O_RDWR);
        lseek(fd, page_tag - extent_tag, SEEK_SET);
        write(fd, bytes, PAGE_SIZE);
        close(fd);
    }
#endif

    pthread_rwlock_wrlock(&page_lock);
    memcpy(page_map->operator[](page_tag).bytes, bytes, PAGE_SIZE);
    page_map->operator[](page_tag).dirty = true;
    pthread_rwlock_unlock(&page_lock);

    return true;
}

/**
 * Read bytes from storage.
 *
 * @param offset The virtual block device offset to read from
 * @param size The number of bytes to read
 * @param bytes The buffer to read bytes into
 * @return The number of bytes read or a negative errno
 */
extern "C" int storage_read(off_t offset, size_t size, uint8_t *bytes)
{
    uint64_t page_tag = offset & (~PAGE_MASK);
    uint16_t size16 = static_cast<uint16_t>(std::min(size, (page_tag + PAGE_SIZE) - offset));

    if (page_tag == static_cast<uint64_t>(offset)) // aligned
    {
        aligned_page_read(page_tag, size16, bytes);
        return size16;
    }
    else // unaligned
    {
        uint8_t page[PAGE_SIZE] = {};
        aligned_page_read(page_tag, PAGE_SIZE, page);
        memcpy(bytes, page + (offset - page_tag), size16);
        return size16;
    }
}

/**
 * Write bytes to storage.
 *
 * @param offset The virtual block device offset to write to
 * @param size The number of bytes to write
 * @param bytes The buffer to read bytes from
 * @return The number of bytes written or a negative errno
 */
extern "C" int storage_write(off_t offset, size_t size, const uint8_t *bytes)
{
    uint64_t page_tag = offset & (~PAGE_MASK);
    uint16_t size16 = static_cast<uint16_t>(std::min(size, (page_tag + PAGE_SIZE) - offset));

    if (page_tag == static_cast<uint64_t>(offset) && size == PAGE_SIZE) // complete page
    {
        aligned_page_write(page_tag, bytes);
        return size16;
    }
    else // unaligned or incomplete page
    {
        uint8_t page[PAGE_SIZE] = {};
        aligned_page_read(page_tag, PAGE_SIZE, page);      // read
        memcpy(page + (offset - page_tag), bytes, size16); // update
        aligned_page_write(page_tag, page);                // write
        return size16;
    }
}

const disk_page &debug_page_map(uint64_t page_tag)
{
    return page_map->operator[](page_tag);
}
