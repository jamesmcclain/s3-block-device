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
#include <cassert>
#include <cstdint>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include <set>

#include "constants.h"
#include "storage.h"
#include "lru.h"
#include "extent.h"
#include "scratch.h"
#include "sync.h"
#include "fullio.h"

struct flush_queue_entry_t
{
    uint64_t tag;
    bool should_remove;

    bool operator<(const flush_queue_entry_t &rhs) const
    {
        return (should_remove > rhs.should_remove) || (tag > rhs.tag);
    }
};

typedef std::set<flush_queue_entry_t> flush_queue_t;

static flush_queue_t *flush_queue = nullptr;
static pthread_mutex_t flush_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static const char *blockdir = nullptr;

void *eviction_queue(void *arg);
void *continuous_queue(void *arg);
void *unqueue(void *arg);

/**
 * Initialize the flush queue.
 */
void queue_init()
{
    if (flush_queue == nullptr)
    {
        flush_queue = new flush_queue_t{};
    }
}

/**
 * Deinitialize the flush queue.
 */
void queue_deinit()
{
    pthread_mutex_lock(&flush_queue_lock);
    if (flush_queue != nullptr)
    {
        delete flush_queue;
        flush_queue = nullptr;
    }
    flush_queue_lock = PTHREAD_MUTEX_INITIALIZER;
}

/**
 * Initialize storage.
 *
 * @param _blockdir A pointer to a string giving the path to the storage directory
 */
void storage_init(const char *_blockdir)
{
    blockdir = _blockdir;
    queue_init();
    extent_init();
    scratch_init();
    lru_init(eviction_queue);
    sync_init(continuous_queue, unqueue);
}

/**
 * Deinitialize storage.
 */
void storage_deinit()
{
    sync_deinit();
    lru_deinit();
    scratch_deinit();
    extent_deinit();
    queue_deinit();
    blockdir = nullptr;
}

/**
 * Bring an extent in from storage to the scratch file.  The caller is
 * assumed to already have a write lock on the extent.
 *
 * @param extent_tag The extent to read
 * @return A boolean indicating whether the extent is now present
 */
bool storage_unflush(uint64_t extent_tag)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    // Acquire resources
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);

    // The extent should be either completely absent, or completely
    // present.  If a hole is found in the extent, then assume it to
    // be completely absent and replace it entirely.
    if (lseek(fd, extent_tag, SEEK_HOLE) < static_cast<off_t>(extent_tag + EXTENT_SIZE))
    {
        char filename[0x100];
        uint8_t *extent_array = new uint8_t[EXTENT_SIZE];
        VSILFILE *handle = NULL;

        memset(extent_array, 0x33, EXTENT_SIZE);

        // If possible, read the extent from remote storage
        sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
        if ((handle = VSIFOpenL(filename, "r")) != NULL)
        {
            for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
            {
                off_t offset = (i * PAGE_SIZE);
                if (VSIFReadL(extent_array + offset, PAGE_SIZE, 1, handle) != 1)
                {
                    free(extent_array);
                    VSIFCloseL(handle);
                    release_scratch_handle(scratch_handle);
                    return false;
                }
            }
            VSIFCloseL(handle);
        }

        // Attempt to write the bytes into the scratch file
        if (lseek(fd, extent_tag, SEEK_SET) == static_cast<off_t>(extent_tag))
        {
            fullwrite(fd, extent_array, EXTENT_SIZE);
        }
        else
        {
            free(extent_array);
            release_scratch_handle(scratch_handle);
            return false;
        }

        // Bytes written to scratch file, so free resources and report
        // success
        free(extent_array);
        release_scratch_handle(scratch_handle);
        return true;
    }
    else
    {
        // Nothing to do, so free resources and report success
        release_scratch_handle(scratch_handle);
        return true;
    }
}

/**
 * Flush an extent to storage from the scratch file.
 *
 * @param page_tag The tag whose entire extent should be flushed
 * @param should_remove Whether or not to remove the extent from the local cache
 * @return Boolean indicating success or failure
 */
bool storage_flush(uint64_t extent_tag, bool should_remove = false)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    // Aquire a write lock on the extent
    extent_spinlock(extent_tag, true);

    // If the extent is clean, leave quickly (possibly punching a hole
    // in the file on the way out, if needed)
    if (extent_clean(extent_tag))
    {
        if (should_remove)
        {
            auto scratch_handle = aquire_scratch_handle();
            fallocate(
                scratch_handle_to_fd(scratch_handle),
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                extent_tag,
                EXTENT_SIZE);
            release_scratch_handle(scratch_handle);
        }
        extent_unlock(extent_tag, true, true);
        return true;
    }

    // Aquire memory
    uint8_t *extent_array = new uint8_t[EXTENT_SIZE];

    // Read the extent from the scratch file into the array
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);
    if (lseek(fd, extent_tag, SEEK_DATA) != static_cast<off_t>(extent_tag))
    {
        extent_unlock(extent_tag, true, true);
        delete extent_array;
        release_scratch_handle(scratch_handle);
        return false;
    }
    fullread(fd, extent_array, EXTENT_SIZE);
    release_scratch_handle(scratch_handle);

    // Open extent file for writing
    char filename[0x100];
    VSILFILE *handle = NULL;
    sprintf(filename, EXTENT_TEMPLATE, blockdir, extent_tag);
    if ((handle = VSIFOpenL(filename, "w")) == NULL)
    {
        delete extent_array;
        extent_unlock(extent_tag, true, false);
        return false;
    }

    // Copy all pages from extent_array
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t offset = i * PAGE_SIZE;

        // Write into extent file
        if (VSIFWriteL(extent_array + offset, PAGE_SIZE, 1, handle) != 1)
        {
            VSIFCloseL(handle);
            delete extent_array;
            extent_unlock(extent_tag, true, false);
            return false;
        }
    }

    if (should_remove)
    {
        // Punch hole
        auto scratch_handle = aquire_scratch_handle();
        fallocate(
            scratch_handle_to_fd(scratch_handle),
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            extent_tag,
            EXTENT_SIZE);
        release_scratch_handle(scratch_handle);
    }

    // Close extent file, release locks, delete array
    VSIFFlushL(handle);
    VSIFCloseL(handle);
    delete extent_array;
    extent_unlock(extent_tag, true, true);

    return true;
}

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
    assert(page_tag == (page_tag & (~PAGE_MASK))); // Assert alignment

    uint64_t extent_tag = page_tag & (~EXTENT_MASK);

    // Note that the page has been touched
    if (should_report)
    {
        lru_report_page(page_tag);
    }

    // Get a write lock
    extent_spinlock(extent_tag, true);

    // Ensure that the extent is in the scratch file
    if (!storage_unflush(extent_tag))
    {
        extent_unlock(extent_tag, true, false);
        return false;
    }

    // Downgrade the write lock to a read lock
    extent_lock_downgrade(extent_tag);

    // Read the bytes
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);
    if (lseek(fd, page_tag, SEEK_DATA) == static_cast<off_t>(page_tag))
    {
        fullread(fd, bytes, size);
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, false, false);
        return true;
    }
    else
    {
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, false, false);
        return false;
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

    uint64_t extent_tag = page_tag & (~EXTENT_MASK);

    // Note that the page has been touched
    lru_report_page(page_tag);

    // Get a write lock
    extent_spinlock(extent_tag, true);

    // Ensure that the extent is in the scratch file
    if (!storage_unflush(extent_tag))
    {
        extent_unlock(extent_tag, true, false);
        return false;
    }

    // Write the bytes
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);
    if (lseek(fd, page_tag, SEEK_DATA) == static_cast<off_t>(page_tag))
    {
        fullwrite(fd, bytes, PAGE_SIZE);
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, true, false);
        return true;
    }
    else
    {
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, true, false);
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

// ------------------------------------------------------------------------

/**
 * Insert an entry into the flush queue.
 *
 * @param extent_tag The tag of the extent in question
 * @param should_remove Whether the extent should be removed from the scratch file
 */
inline void flush_queue_insert(uint64_t extent_tag, bool should_remove)
{
    pthread_mutex_lock(&flush_queue_lock);
    flush_queue->insert(flush_queue_entry_t{extent_tag, should_remove});
    pthread_mutex_unlock(&flush_queue_lock);
}

/**
 * Flush an evicted extent.
 *
 * @param arg The tag of the extent to flush
 * @return Always nullptr
 */
void *eviction_queue(void *arg)
{
    uint64_t tag = reinterpret_cast<uint64_t>(arg);

    flush_queue_insert(tag, true);
    return nullptr;
}

/**
 * Continuously flush all dirty pages to storage.
 *
 * @param arg Unused
 * @return Always nullptr
 */
void *continuous_queue(void *arg)
{
    while (sync_thread_continue)
    {
        uint64_t extent_tag;

        if (extent_first_dirty_unreferenced(&extent_tag))
        {
            flush_queue_insert(extent_tag, false);
        }
        else
        {
            sleep(1);
        }
    }
    return nullptr;
}

/**
 * Unqueue extents: write them from the scratch file to storage.
 *
 * @param arg Unused
 * @return Always nullptr
 */
void *unqueue(void *arg)
{
    while (sync_thread_continue)
    {
        pthread_mutex_lock(&flush_queue_lock);
        if (!flush_queue->empty())
        {
            auto itr = flush_queue->cbegin();
            auto tag = itr->tag;
            auto should_remove = itr->should_remove;
#if 0
            fprintf(stderr, "XXX size=%ld tag=%016lx should_remove=%s\n", flush_queue->size(), tag, should_remove ? "true" : "false");
#endif
            flush_queue->erase(itr);
            pthread_mutex_unlock(&flush_queue_lock);
            storage_flush(tag, should_remove);
        }
        else
        {
            pthread_mutex_unlock(&flush_queue_lock);
            sleep(1);
        }
    }
    return nullptr;
}
