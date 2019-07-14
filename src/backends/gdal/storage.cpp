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

#include <algorithm>
#include <functional>
#include <vector>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <gdal.h>
#include <cpl_vsi.h>

#include <pthread.h>

#include "constants.h"
#include "storage.h"
#include "lru.h"
#include "extent.h"
#include "fullio.h"

// Block directory name
static const char *blockdir = nullptr;

// Scratch file
typedef struct
{
    pthread_mutex_t lock;
    int fd;
} locked_fd_t;
typedef std::vector<locked_fd_t> locked_fd_vector_t;
static locked_fd_vector_t locked_fd_vector;

// Sync thread
static bool sync_thread_continue = false;
static pthread_t sync_thread;
static int storage_sync_interval = SYNC_INTERVAL_DEFAULT;

#if 0
// Extent entry reaper thread
static bool reaper_thread_continue = false;
static pthread_t reaper_thread;
#endif

// ------------------------------------------------------------------------

// XXX clean extent entry reaper thread

// ------------------------------------------------------------------------

/**
 * Acquire a handle to a file descriptor for the scratch file.
 *
 * @return A handle that is mappable to a file descriptor.
 */
static size_t aquire_scratch_handle()
{
    for (size_t i = 0; true; ++i)
    {
        size_t handle = i % SCRATCH_DESCRIPTORS;
        pthread_mutex_t *lock = &(locked_fd_vector[handle].lock);

        if (pthread_mutex_trylock(lock) == 0)
        {
            return handle;
        }
    }
}

/**
 * Translate a previously-acquired handle into file descriptor for the
 * scratch file.
 *
 * @param handle A previously-acquired handle
 */
static int scratch_handle_to_fd(size_t handle)
{
    return locked_fd_vector[handle].fd;
}

/**
 * Release a scratch file handle.
 *
 * @param handle The handle to release
 */
static void release_scratch_handle(size_t handle)
{
    pthread_mutex_t *lock = &(locked_fd_vector[handle].lock);
    pthread_mutex_unlock(lock);
}

// ------------------------------------------------------------------------

void *delayed_flush_extent(void *arg);

/**
 * Initialize storage.
 *
 * @param _blockdir A pointer to a string giving the path to the storage directory
 */
void storage_init(const char *_blockdir)
{

    // Note blockdir
    blockdir = _blockdir;

    // Extent locking
    extent_init();

    // Scratch file
    {
        char scratch_filename[0x100];

        // Initialize file descriptor list
        sprintf(scratch_filename, SCRATCH_TEMPLATE, getpid());
        for (size_t i = 0; i < SCRATCH_DESCRIPTORS; ++i)
        {
            locked_fd_vector.push_back(locked_fd_t{
                PTHREAD_MUTEX_INITIALIZER,
                open(scratch_filename, O_RDWR | O_CREAT, S_IRWXU)});
        }

        // Unlink scratch file if not told to keep it
        if (getenv(S3BD_KEEP_SCRATCH_FILE) == nullptr)
        {
            unlink(scratch_filename);
        }
    }

    // LRU cache
    {
        size_t local_cache_megabytes = LOCAL_CACHE_DEFAULT_MEGABYTES;
        size_t local_cache_extents;
        const char *str;

        if ((str = getenv(S3BD_LOCAL_CACHE_MEGABYTES)) != nullptr)
        {
            sscanf(str, "%lu", &local_cache_megabytes);
        }
        local_cache_extents = (local_cache_megabytes * (1 << 20)) / EXTENT_SIZE;
        lru_init(local_cache_extents, delayed_flush_extent);
    }

    // Start the storage flush thread
    {
        const char *str;

        if ((str = getenv(S3BD_SYNC_INTERVAL)) != nullptr)
        {
            sscanf(str, "%d", &storage_sync_interval);
        }
        sync_thread_continue = true;
        pthread_create(&sync_thread, NULL, storage_flush, nullptr);
    }
}

/**
 * Deinitialize storage.
 */
void storage_deinit()
{
    sync_thread_continue = false;
    pthread_join(sync_thread, nullptr);

    for (size_t i = 0; i < SCRATCH_DESCRIPTORS; ++i)
    {
        close(locked_fd_vector[i].fd);
    }
    locked_fd_vector.clear();
    extent_deinit();
    lru_deinit();
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
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    uint8_t *extent_array = new uint8_t[EXTENT_SIZE];

    // If the extent is clean, leave quickly (possibly punching a hole if needed)
    extent_spin_lock(extent_tag, true);
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
    extent_unlock(extent_tag, true, false);

    // Bring contents of all pages into "extent_array"
    for (unsigned int i = 0; i < PAGES_PER_EXTENT; ++i)
    {
        uint64_t inner_offset = i * PAGE_SIZE;
        bool should_report = !should_remove; // If pages are about to be removed, then no reason to report them as touched
        aligned_page_read(
            extent_tag + inner_offset,
            PAGE_SIZE,
            extent_array + inner_offset,
            should_report);
    }

    // Open extent file for writing
    extent_spin_lock(extent_tag, true);
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
            VSIFFlushL(handle);
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
    extent_unlock(extent_tag, true, true);
    delete extent_array;

    return true;
}

/**
 * Synchronously flush all dirty pages to storage.
 *
 * @return nullptr
 */
void *storage_flush(void *arg)
{
    while (sync_thread_continue)
    {
        uint64_t extent_tag;

        if (extent_first_dirty_and_unreferenced(&extent_tag))
        {
            flush_extent(extent_tag);
        }
        else
        {
            sleep(storage_sync_interval);
        }
    }
    return nullptr;
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

    // Acquire resources
    extent_spin_lock(extent_tag, false);
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);

    if (lseek(fd, page_tag, SEEK_DATA) == static_cast<off_t>(page_tag)) // If page already exists ...
    {
        fullread(fd, bytes, size);
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, false, false);
        return true;
    }
    else // If the page does not yet exist ...
    {
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

            if (lseek(fd, inner_page_tag, SEEK_DATA) == static_cast<off_t>(inner_page_tag)) // If the page already exists ...
            {
                if (inner_page_tag == page_tag)
                {
                    fullread(fd, bytes, size);
                }
                continue;
            }
            else // If the page does not yet exist ...
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
                        release_scratch_handle(scratch_handle);
                        extent_unlock(extent_tag, false, false);
                        delete scratch_page;
                        return false;
                    }
                    if (VSIFReadL(scratch_page, PAGE_SIZE, 1, handle) != 1)
                    {
                        release_scratch_handle(scratch_handle);
                        extent_unlock(extent_tag, false, false);
                        delete scratch_page;
                        return false;
                    }

                    // Attempt to write the page to the local scratch file
                    if (lseek(fd, inner_page_tag, SEEK_SET) != static_cast<off_t>(inner_page_tag))
                    {
                        release_scratch_handle(scratch_handle);
                        extent_unlock(extent_tag, false, false);
                        delete scratch_page;
                        return false;
                    }
                    fullwrite(fd, scratch_page, PAGE_SIZE);

                    if (inner_page_tag == page_tag)
                    {
                        memcpy(bytes, scratch_page, size);
                    }
                }
            }
        }

        // Release handle and other resources
        if (handle != NULL)
        {
            VSIFCloseL(handle);
        }
        release_scratch_handle(scratch_handle);
        extent_unlock(extent_tag, false, false);
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

    uint64_t extent_tag = page_tag & (~EXTENT_MASK);

    // Note that the page has been touched
    lru_report_page(page_tag);

    // Acquire resources
    extent_spin_lock(extent_tag, true);
    auto scratch_handle = aquire_scratch_handle();
    int fd = scratch_handle_to_fd(scratch_handle);

    if (lseek(fd, page_tag, SEEK_SET) == static_cast<off_t>(page_tag))
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
 * Flush the given extent after a short delay.  The delay is to
 * prevent thrashing if there is cache pressure and, for example, a
 * page has been read as part of getting a complete extent to perform
 * an extent write as part of an eviction.
 *
 * @param arg The tag of the extent to flush
 * @return Always nullptr
 */
void *delayed_flush_extent(void *arg)
{
    uint64_t tag = reinterpret_cast<uint64_t>(arg);

    lru_aquire_thread();
    sleep(1);
    flush_extent(tag, true);
    lru_release_thread();
    return nullptr;
}

