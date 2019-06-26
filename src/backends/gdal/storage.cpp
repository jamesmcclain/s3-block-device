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
#include <set>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include "storage.h"

typedef std::set<uint64_t> tag_set_t;
tag_set_t *tag_set = nullptr;

static const char *blockdir = nullptr;

static pthread_rwlock_t scratch_lock = PTHREAD_RWLOCK_INITIALIZER;
int scratch_fd = -1;

/**
 * Initialize storage.
 *
 * @param _blockdir A pointer to a string giving the path to the storage directory
 */
extern "C" void storage_init(const char *_blockdir)
{
    assert(_blockdir != nullptr);
    blockdir = _blockdir;

    scratch_fd = open(SCRATCH_FILE, O_RDWR | O_CREAT, S_IRWXU);

    if (tag_set == nullptr)
    {
        tag_set = new tag_set_t();
    }

    if (tag_set == nullptr)
    {
        throw std::bad_alloc();
    }
}

/**
 * Deinitialize storage.
 */
extern "C" void storage_deinit()
{
    if (tag_set != nullptr)
    {
        delete tag_set;
        tag_set = nullptr;
    }
    close(scratch_fd);
#if 0
    unlink(SCRATCH_FILE);
#endif
}

#if 0
/**
 * Flush a page and all of its extent-mates to storage.  Only one
 * instance of this function should ever be active at once.
 */
bool flush_page_and_friends(uint64_t page_tag)
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
#endif

/**
 * Attempt to read and return a page or less of data.
 *
 * @param page_tag The tag of the page to read from
 * @param size The number of bytes to read (must be <= the size of a page)
 * @param bytes The array in which to return the bytes
 * @return Boolean indicating success or failure
 */
bool aligned_page_read(uint64_t page_tag, uint16_t size, uint8_t *bytes)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    pthread_rwlock_rdlock(&scratch_lock);
    if (tag_set->count(page_tag) > 0) // If page should exist ...
    {
        if (lseek(scratch_fd, page_tag, SEEK_DATA) == static_cast<off_t>(page_tag)) // ... and page exists
        {
            assert(read(scratch_fd, bytes, size) == size); // XXX ensure full read
            pthread_rwlock_unlock(&scratch_lock);
            return true;
        }
        else // ... otherwise if it should exist but does not
        {
            pthread_rwlock_unlock(&scratch_lock);
            return false;
        }
    }
    else // If the page should not exist
    {
        pthread_rwlock_unlock(&scratch_lock);
        memset(bytes, 0, size);
        return true;
    }
}

/**
 * Attempt to write a whole page of data.
 *
 * @param page_tag The tag of the page to write from
 * @param size The number of bytes to write (must be <= the size of a page)
 * @param bytes The array from which to write the bytes
 * @return Boolean indicating success or failure
 */
bool aligned_whole_page_write(uint64_t page_tag, const uint8_t *bytes)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    pthread_rwlock_wrlock(&scratch_lock);
    if (tag_set->count(page_tag) > 0) // If page should exist ...
    {
        if (lseek(scratch_fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag)) // ... and page exists
        {
            assert(write(scratch_fd, bytes, PAGE_SIZE) == PAGE_SIZE); // XXX ensure full write
            pthread_rwlock_unlock(&scratch_lock);
            return true;
        }
        else // ... otherwise if it should exist but does not
        {
            pthread_rwlock_unlock(&scratch_lock);
            return false;
        }
    }
    else // If the page should not exist, yet ...
    {
        tag_set->insert(page_tag);
        if (lseek(scratch_fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag)) // ... and the seek succeeds
        {
            write(scratch_fd, bytes, PAGE_SIZE);
            pthread_rwlock_unlock(&scratch_lock);
            return true;
        }
        else // ... otherwise  if the seek fails
        {
            pthread_rwlock_unlock(&scratch_lock);
            return false;
        }
    }
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

    if (page_tag == static_cast<uint64_t>(offset)) // If aligned read ...
    {
        int bytes_read = 0;
        while (size > 0)
        {
            auto size2 = std::min(size, PAGE_SIZE);
            if (aligned_page_read(page_tag, size2, bytes))
            {
                page_tag += size2;
                size -= size2;
                bytes += size2;
                bytes_read += size2;
            }
            else
            {
                break;
            }
        }
        return bytes_read;
    }
    else // If unaligned read ...
    {
        auto diff = offset - page_tag;
        auto size2 = std::min(size, PAGE_SIZE - diff);
        uint8_t page[PAGE_SIZE];

        aligned_page_read(page_tag, PAGE_SIZE, page);
        memcpy(bytes, page + diff, size2);
        if (size2 == size)
        {
            return size;
        }
        else
        {
            return size2 + storage_read(page_tag + PAGE_SIZE, size - size2, bytes + size2);
        }
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

    if (page_tag == static_cast<uint64_t>(offset) && (size % PAGE_SIZE == 0)) // If writing a complete page ...
    {
        int bytes_written = 0;
        while (size > 0)
        {
            if (aligned_whole_page_write(page_tag, bytes))
            {
                page_tag += PAGE_SIZE;
                size -= PAGE_SIZE;
                bytes += PAGE_SIZE;
                bytes_written += PAGE_SIZE;
            }
            else
            {
                break;
            }
        }
        return bytes_written;
    }
    else // If writing an unaligned and/or incomplete page ...
    {
        auto diff = offset - page_tag;
        auto size2 = std::min(size, PAGE_SIZE - diff);
        uint8_t page[PAGE_SIZE] = {};

        aligned_page_read(page_tag, PAGE_SIZE, page); // read
        memcpy(page + diff, bytes, size2);            // update
        aligned_whole_page_write(page_tag, page);     // write

        if (size2 == size)
        {
            return size;
        }
        else
        {
            return size2 + storage_write(page_tag + PAGE_SIZE, size - size2, bytes + size2);
        }
    }
}
