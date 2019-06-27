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
#include <cstdlib>
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

static const char *blockdir = nullptr;

typedef std::set<uint64_t> tag_set_t;
static tag_set_t *tag_set = nullptr;
static pthread_rwlock_t tag_set_lock;

static int scratch_write_fd = -1;
static pthread_mutex_t scratch_write_lock;

static constexpr int SCRATCH_FD_COUNT = 8;
static int scratch_read_fd[SCRATCH_FD_COUNT];
static pthread_mutex_t scratch_read_mutex[SCRATCH_FD_COUNT];

/**
 * Initialize storage.
 *
 * @param _blockdir A pointer to a string giving the path to the storage directory
 */
void storage_init(const char *_blockdir)
{
    char scratch_filename[0x100];

    assert(_blockdir != nullptr);
    blockdir = _blockdir;

    tag_set_lock = PTHREAD_RWLOCK_INITIALIZER;

    scratch_write_lock = PTHREAD_MUTEX_INITIALIZER;
    sprintf(scratch_filename, SCRATCH_TEMPLATE, getpid());
    scratch_write_fd = open(scratch_filename, O_RDWR | O_CREAT, S_IRWXU);

    for (int i = 0; i < SCRATCH_FD_COUNT; ++i)
    {
        scratch_read_fd[i] = open(scratch_filename, O_RDONLY);
        scratch_read_mutex[i] = PTHREAD_MUTEX_INITIALIZER;
    }

    if (getenv("S3BD_KEEP_SCRATCH_FILE") == nullptr)
    {
        unlink(scratch_filename);
    }

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
void storage_deinit()
{
    if (tag_set != nullptr)
    {
        delete tag_set;
        tag_set = nullptr;
    }
    close(scratch_write_fd);
}

/**
 * Flush a page and all of its extent-mates to storage.  Only one
 * instance of this function should ever be active at once.
 *
 * @param page_tag The tag whose entire extent should be flushed
 * @return Boolean indicating success or failure
 */
bool flush_page_and_friends(uint64_t page_tag)
{
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    uint8_t *extent = new uint8_t[EXTENT_SIZE];
    uint64_t extent_tag = (page_tag & (~EXTENT_MASK));

    // Bring all pages into the array `extent`
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t inner_offset = i * PAGE_SIZE;
        aligned_page_read(extent_tag + inner_offset, PAGE_SIZE, extent + inner_offset);
    }

    // Open extent file
    char filename[0x100];
    VSILFILE *handle = NULL;
    sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
    if ((handle = VSIFOpenL(filename, "w")) == NULL)
    {
        delete extent;
        return false;
    }

    // Copy all pages from the array `extent`, delete them from the scratch file
    pthread_rwlock_wrlock(&tag_set_lock);
    pthread_mutex_lock(&scratch_write_lock);
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t offset = i * PAGE_SIZE;
        uint64_t inner_page_tag = extent_tag + offset;

        // Write into extent file
        if (VSIFWriteL(extent + offset, PAGE_SIZE, 1, handle) != 1)
        {
            pthread_mutex_unlock(&scratch_write_lock);
            pthread_rwlock_unlock(&tag_set_lock);
            VSIFFlushL(handle);
            VSIFCloseL(handle);
            delete extent;
            return false;
        }

        // Remove entry from scratch file
        fallocate(scratch_write_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, inner_page_tag, PAGE_SIZE);

        // Remove entry from tag set
        tag_set->erase(inner_page_tag);
    }
    pthread_mutex_unlock(&scratch_write_lock);
    pthread_rwlock_unlock(&tag_set_lock);

    VSIFFlushL(handle);
    VSIFCloseL(handle);

    delete extent;

    return true;
}

/**
 * Flush all pages to storage.
 * 
 * @return 0 on success, -1 on failure
 */
int storage_flush()
{
    while (true)
    {
        pthread_rwlock_rdlock(&tag_set_lock);
        auto itr = tag_set->begin();
        if (itr == tag_set->end())
        {
            pthread_rwlock_unlock(&tag_set_lock);
            return 0;
        }
        else
        {
            uint64_t page_tag = *itr;
            pthread_rwlock_unlock(&tag_set_lock);
            flush_page_and_friends(page_tag);
        }
    }
    return 0;
}

#define GET_FD(index) for (index = 0; pthread_mutex_trylock(&scratch_read_mutex[index % SCRATCH_FD_COUNT]); ++index)
#define USE_FD(index) (scratch_read_fd[index % SCRATCH_FD_COUNT])
#define RELEASE_FD(index) pthread_mutex_unlock(&scratch_read_mutex[index % SCRATCH_FD_COUNT])

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

    pthread_rwlock_rdlock(&tag_set_lock);
    if (tag_set->count(page_tag) > 0) // If page should exist ...
    {
        int index = 0;

        pthread_rwlock_unlock(&tag_set_lock);
        GET_FD(index);
        if (lseek(USE_FD(index), page_tag, SEEK_DATA) == static_cast<off_t>(page_tag)) // ... and page exists
        {
            assert(read(USE_FD(index), bytes, size) == size); // XXX ensure full read
            RELEASE_FD(index);
            return true;
        }
        else // ... otherwise if it should exist but does not
        {
            RELEASE_FD(index);
            return false;
        }
    }
    else // If the page should not exist yet ...
    {
        uint64_t extent_tag = page_tag & (~EXTENT_MASK);
        char filename[0x100];
        VSILFILE *handle = NULL;

        pthread_rwlock_unlock(&tag_set_lock); // drop read lock

        // Attempt to open the far-away extent file ...
        sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
        if ((handle = VSIFOpenL(filename, "r")) == NULL) // ... if it doesn't exist, return
        {
            memset(bytes, 0x33, size);
            return true;
        }

        pthread_rwlock_wrlock(&tag_set_lock); // acquire write lock

        // Loop through the extent file and attempt to read all not-already-present pages ...
        uint8_t *scratch_page = new uint8_t[PAGE_SIZE];
        for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
        {
            uint64_t offset = (i * PAGE_SIZE);
            uint64_t inner_page_tag = extent_tag + offset;

            if (tag_set->count(inner_page_tag) == 0)
            {
                // Attempt to seek to the offset of the page in the extent file ...
                if (VSIFSeekL(handle, offset, SEEK_SET) != 0)
                {
                    pthread_rwlock_unlock(&tag_set_lock);
                    return false;
                }
                // Attempt to read the page from the extent file ...
                if (VSIFReadL(scratch_page, PAGE_SIZE, 1, handle) != 1)
                {
                    pthread_rwlock_unlock(&tag_set_lock);
                    delete scratch_page;
                    return false;
                }
                // Attempt to seek to the page in the local scratch file ...
                pthread_mutex_lock(&scratch_write_lock);
                if (lseek(scratch_write_fd, inner_page_tag, SEEK_SET) != static_cast<off_t>(inner_page_tag))
                {
                    pthread_mutex_unlock(&scratch_write_lock);
                    pthread_rwlock_unlock(&tag_set_lock);
                    delete scratch_page;
                    return false;
                }
                // Write the page to the local scratch file
                assert(write(scratch_write_fd, scratch_page, PAGE_SIZE) == PAGE_SIZE); // XXX ensure full write
                pthread_mutex_unlock(&scratch_write_lock);
                tag_set->insert(inner_page_tag);
            }
        }

        int index = 0;

        GET_FD(index);
        if (lseek(USE_FD(index), page_tag, SEEK_DATA) == static_cast<off_t>(page_tag)) // Now seek to the data ...
        {
            assert(read(USE_FD(index), bytes, size) == size); // XXX ensure full read
            RELEASE_FD(index);
            pthread_rwlock_unlock(&tag_set_lock);
            return true;
        }
        else
        {
            RELEASE_FD(index);
            pthread_rwlock_unlock(&tag_set_lock);
            return false;
        }

        delete scratch_page;

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

    pthread_rwlock_wrlock(&tag_set_lock); // XXX would like to upgrade
    pthread_mutex_lock(&scratch_write_lock);
    if (tag_set->count(page_tag) > 0) // If page is already in the tag set ...
    {
        pthread_rwlock_unlock(&tag_set_lock);
        if (lseek(scratch_write_fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag)) // ... and page position is seekable
        {
            assert(write(scratch_write_fd, bytes, PAGE_SIZE) == PAGE_SIZE); // XXX ensure full write
            pthread_mutex_unlock(&scratch_write_lock);
            return true;
        }
        else // ... otherwise if it should exist but does not
        {
            pthread_mutex_unlock(&scratch_write_lock);
            return false;
        }
    }
    else // If the page is not already in the tag set ...
    {
        tag_set->insert(page_tag);
        if (lseek(scratch_write_fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag)) // ... and the seek succeeds
        {
            assert(write(scratch_write_fd, bytes, PAGE_SIZE) == PAGE_SIZE); //XXX should ensure complete write
            pthread_mutex_unlock(&scratch_write_lock);
            pthread_rwlock_unlock(&tag_set_lock);
            return true;
        }
        else // ... otherwise  if the seek fails
        {
            pthread_mutex_unlock(&scratch_write_lock);
            pthread_rwlock_unlock(&tag_set_lock);
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
