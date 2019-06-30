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
#include <atomic>
#include <cassert>
#include <set>

#include <boost/compute/detail/lru_cache.hpp>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include "storage.h"
#include "fullio.h"

typedef boost::compute::detail::lru_cache<uint64_t, bool> lru_cache_t;
static std::atomic<int> background_threads{0};
static lru_cache_t *lru_cache = nullptr;
static pthread_mutex_t cache_lock;

static const char *blockdir = nullptr;

typedef std::set<uint64_t> dirty_extent_set_t;
static dirty_extent_set_t *dirty_extent_set = nullptr;
static pthread_rwlock_t dirty_extent_lock;

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

    cache_lock = PTHREAD_MUTEX_INITIALIZER;

    dirty_extent_lock = PTHREAD_RWLOCK_INITIALIZER;

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

    if (lru_cache == nullptr)
    {
        if ((lru_cache = new lru_cache_t(256 / 4)) == nullptr)
        {
            throw new std::bad_alloc();
        }
    }

    if (dirty_extent_set == nullptr)
    {
        if ((dirty_extent_set = new dirty_extent_set_t()) == nullptr)
        {
            throw std::bad_alloc();
        }
    }
}

/**
 * Deinitialize storage.
 */
void storage_deinit()
{
    if (lru_cache != nullptr)
    {
        delete lru_cache;
        lru_cache = nullptr;
    }

    if (dirty_extent_set != nullptr)
    {
        delete dirty_extent_set;
        dirty_extent_set = nullptr;
    }
    close(scratch_write_fd);
}

/**
 * Flush an extent to storage.  Only one instance of this function
 * should ever be active at once.
 *
 * @param page_tag The tag whose entire extent should be flushed
 * @param should_remove Whether or not to remove the extent from the local cache
 * @return Boolean indicating success or failure
 */
bool flush_extent(uint64_t extent_tag, bool should_remove = false)
{
    uint8_t *extent = new uint8_t[EXTENT_SIZE];

    assert(extent_tag == (extent_tag & (~EXTENT_MASK))); // Assert alignment

    // Ensure that the extent is actually dirty
    pthread_rwlock_rdlock(&dirty_extent_lock);
    if (dirty_extent_set->count(extent_tag) < 1)
    {
        pthread_rwlock_unlock(&dirty_extent_lock);
        return true;
    }
    pthread_rwlock_unlock(&dirty_extent_lock);

    // Bring all pages into the array `extent`
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t inner_offset = i * PAGE_SIZE;
        bool should_report = !should_remove; // Pages are about to be removed, so no reason to report them as touched
        aligned_page_read(extent_tag + inner_offset, PAGE_SIZE, extent + inner_offset, should_report);
    }

    // Acquire locks
    pthread_rwlock_wrlock(&dirty_extent_lock);
    pthread_mutex_lock(&scratch_write_lock);

    // Open extent file for writing
    // XXX extent only being written to by one thread (currently guranteed by scratch_write_lock)
    char filename[0x100];
    VSILFILE *handle = NULL;
    sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
    if ((handle = VSIFOpenL(filename, "w")) == NULL)
    {
        delete extent;
        pthread_mutex_unlock(&scratch_write_lock);
        pthread_rwlock_unlock(&dirty_extent_lock);
        return false;
    }

    // Copy all pages from the array `extent`
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t offset = i * PAGE_SIZE;

        // Write into extent file
        if (VSIFWriteL(extent + offset, PAGE_SIZE, 1, handle) != 1)
        {
            pthread_mutex_unlock(&scratch_write_lock);
            pthread_rwlock_unlock(&dirty_extent_lock);
            VSIFFlushL(handle);
            VSIFCloseL(handle);
            delete extent;
            return false;
        }
    }

    if (should_remove)
    {
        // Remove entry from scratch file
        fallocate(scratch_write_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, extent_tag, EXTENT_SIZE);
    }

    // Remove entry from dirty set
    dirty_extent_set->erase(extent_tag);

    // Close extent file
    VSIFFlushL(handle);
    VSIFCloseL(handle);

    // Release locks
    pthread_mutex_unlock(&scratch_write_lock);
    pthread_rwlock_unlock(&dirty_extent_lock);

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
        pthread_rwlock_rdlock(&dirty_extent_lock);
        auto itr = dirty_extent_set->begin();
        if (itr == dirty_extent_set->end())
        {
            pthread_rwlock_unlock(&dirty_extent_lock);
            return 0;
        }
        else
        {
            uint64_t extent_tag = *itr;
            pthread_rwlock_unlock(&dirty_extent_lock);
            flush_extent(extent_tag);
        }
    }
    return 0;
}

#define GET_FD(index) \
    for (index = 0; pthread_mutex_trylock(&scratch_read_mutex[index % SCRATCH_FD_COUNT]); ++index)
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
bool aligned_page_read(uint64_t page_tag, uint16_t size, uint8_t *bytes, bool should_report)
{
    int index = 0;

    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    // Note that the page has been touched
    if (should_report)
    {
        pthread_mutex_lock(&cache_lock);
        lru_cache->insert(page_tag & ~EXTENT_MASK, true);
        pthread_mutex_unlock(&cache_lock);
    }

    GET_FD(index); // Aquire a file descriptor for reading

    if (lseek(USE_FD(index), page_tag, SEEK_DATA) == static_cast<off_t>(page_tag)) // If page exists ...
    {
        fullread(USE_FD(index), bytes, size);
        RELEASE_FD(index);
        return true;
    }
    else // If the page does not yet exist ...
    {
        uint64_t extent_tag = page_tag & (~EXTENT_MASK);
        char filename[0x100];
        VSILFILE *handle = NULL;
        uint8_t *scratch_page = new uint8_t[PAGE_SIZE];

        // Attempt to open the far-away extent file
        sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
        handle = VSIFOpenL(filename, "r");

        // Loop through every page in the extent
        for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
        {
            uint64_t offset = (i * PAGE_SIZE);
            uint64_t inner_page_tag = extent_tag + offset;

            if (lseek(USE_FD(index), inner_page_tag, SEEK_DATA) == static_cast<off_t>(inner_page_tag)) // If the page already exists ...
            {
                if (inner_page_tag == page_tag)
                {
                    fullread(USE_FD(index), bytes, size);
                }
                continue;
            }
            else // If the page does not exist ...
            {
                if (handle == NULL) // .. and the extent file was not openable ...
                {
                    if (inner_page_tag == page_tag)
                    {
                        memset(bytes, 0x33, size);
                    }
                }
                else // ... and the extent was openable ...
                {
                    if (VSIFSeekL(handle, offset, SEEK_SET) != 0)
                    {
                        RELEASE_FD(index);
                        delete scratch_page;
                        return false;
                    }
                    if (VSIFReadL(scratch_page, PAGE_SIZE, 1, handle) != 1)
                    {
                        RELEASE_FD(index);
                        delete scratch_page;
                        return false;
                    }

                    pthread_mutex_lock(&scratch_write_lock);
                    if (lseek(scratch_write_fd, inner_page_tag, SEEK_SET) != static_cast<off_t>(inner_page_tag))
                    {
                        pthread_mutex_unlock(&scratch_write_lock);
                        RELEASE_FD(index);
                        delete scratch_page;
                        return false;
                    }
                    // write the page to the local scratch file
                    fullwrite(scratch_write_fd, scratch_page, PAGE_SIZE);
                    pthread_mutex_unlock(&scratch_write_lock);

                    if (inner_page_tag == page_tag)
                    {
                        memcpy(bytes, scratch_page, size);
                    }
                }
            }
        }

        RELEASE_FD(index);

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

    // Note that the page has been touched
    pthread_mutex_lock(&cache_lock);
    lru_cache->insert(page_tag & ~EXTENT_MASK, false);
    pthread_mutex_unlock(&cache_lock);

    uint64_t extent_tag = page_tag & (~EXTENT_MASK);

    pthread_mutex_lock(&scratch_write_lock);
    if (lseek(scratch_write_fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag))
    {
        fullwrite(scratch_write_fd, bytes, PAGE_SIZE);
        pthread_mutex_unlock(&scratch_write_lock);

        pthread_rwlock_wrlock(&dirty_extent_lock);
        if (dirty_extent_set->count(extent_tag) < 1)
        {
            dirty_extent_set->insert(extent_tag);
        }
        pthread_rwlock_unlock(&dirty_extent_lock);

        return true;
    }
    else
    {
        pthread_mutex_unlock(&scratch_write_lock);
        return false;
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

/**
 * Flush the given extent after a short delay.
 *
 * @param _tag The tag of the extent to flush
 * @return Always nullptr
 */
void *delayed_flush_extent(void *_tag)
{
    uint64_t tag = reinterpret_cast<uint64_t>(_tag);

    background_threads++;
    sleep(1);
    flush_extent(tag, true);
    background_threads--;
    return nullptr;
}

/**
 * Specialization of the lru_cache_t::evict method.
 */
template <>
void lru_cache_t::evict()
{
    pthread_t thread;
    uint64_t tag;

    // evict item from the end of most recently used list
    typename list_type::iterator i = --m_list.end();
    tag = *i;
    m_map.erase(*i);
    m_list.erase(i);

    fprintf(stderr, "XXY %s:%d %016lX\n", __FILE__, __LINE__, tag);
    while (background_threads >= APPROX_MAX_BACKGROUND_THREADS)
    {
    }
    pthread_create(&thread, NULL, delayed_flush_extent, reinterpret_cast<void *>(tag));
    pthread_detach(thread);
}
