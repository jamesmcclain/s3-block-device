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

#include <unistd.h>
#include <assert.h>

#include <pthread.h>

#include <atomic>
#include <map>
#include <functional>
#include <vector>

#include "constants.h"
#include "extent.h"

typedef struct
{
    bool dirty;
    int refcount;
} extent_entry_t;

typedef std::map<uint64_t, extent_entry_t> extent_map_t;

typedef struct
{
    pthread_mutex_t lock;
    extent_map_t entries;
} extent_bucket_t;

typedef std::vector<extent_bucket_t> extent_buckets_t;

typedef std::hash<uint64_t> extent_bucket_hash_t;

static extent_bucket_hash_t extent_bucket_hash = extent_bucket_hash_t{};
static extent_buckets_t *extent_buckets = nullptr;
static std::atomic<size_t> moop{0};

/**
 * Initialize extent tracking.
 */
void extent_init()
{
    if (extent_buckets == nullptr)
    {
        extent_buckets = new extent_buckets_t{};
        for (size_t i = 0; i < EXTENT_BUCKETS; ++i)
        {
            extent_buckets->push_back(extent_bucket_t{
                PTHREAD_MUTEX_INITIALIZER,
                extent_map_t{}});
        }
    }
}

/**
 * Deinitialize extent tracking.
 */
void extent_deinit()
{
    if (extent_buckets != nullptr)
    {
        delete extent_buckets;
        extent_buckets = nullptr;
    }
}

/**
 * Get lock on an extent.
 *
 * @param extent_tag The tag of the extent
 * @param wrlock True if write lock requested, false if read lock requested
 * @return A boolean indicating success or failure
 */
bool extent_lock(uint64_t extent_tag, bool wrlock)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    auto index = extent_bucket_hash(extent_tag) % extent_buckets->size();
    auto &bucket = extent_buckets->operator[](index);

    pthread_mutex_lock(&bucket.lock);
    auto itr = bucket.entries.find(extent_tag);
    if (itr != bucket.entries.end()) // Existing entry
    {
        if ((wrlock && itr->second.refcount != 0) || (!wrlock && itr->second.refcount < 0)) // Cannot get write lock || write lock already held
        {
            pthread_mutex_unlock(&bucket.lock);
            return false;
        }
        else // No write locks currently held
        {
            if (wrlock) // Write lock
            {
                itr->second.dirty = true;
                itr->second.refcount--;
            }
            else // Read lock
            {
                itr->second.refcount++;
            }
            pthread_mutex_unlock(&bucket.lock);
            return true;
        }
    }
    else // No existing entry, make one
    {
        if (wrlock) // Write lock
        {
            // "true" means "dirty", "-1" means "write lock held"
            bucket.entries.insert(std::make_pair(extent_tag, extent_entry_t{true, -1}));
        }
        else // Read lock
        {
            // "false" means "not dirty", "+1" means "one read lock held"
            bucket.entries.insert(std::make_pair(extent_tag, extent_entry_t{false, +1}));
        }
        pthread_mutex_unlock(&bucket.lock);
        return true;
    }
}

void extent_spinlock(uint64_t extent_tag, bool wrlock)
{
    while (!extent_lock(extent_tag, wrlock))
    {
        sleep(0);
    }
}

/**
 * Downgrade a write lock to a read lock.
 *
 * @param extent_tag The tag of the extent
 */
void extent_lock_downgrade(uint64_t extent_tag)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    auto index = extent_bucket_hash(extent_tag) % extent_buckets->size();
    auto &bucket = extent_buckets->operator[](index);

    pthread_mutex_lock(&bucket.lock);
    auto itr = bucket.entries.find(extent_tag);
    assert(itr != bucket.entries.end());
    assert(itr->second.refcount == -1);
    itr->second.refcount = 1;
    pthread_mutex_unlock(&bucket.lock);
}

/**
 * Release lock on an extent.
 *
 * @param extent_tag The tag of the extent
 * @param wrlock True if dropping write lock, false if dropping read lock
 * @param mark_clean True if wrlock and extent should be marked clean, false otherwise
 */
void extent_unlock(uint64_t extent_tag, bool wrlock, bool mark_clean)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    auto index = extent_bucket_hash(extent_tag) % extent_buckets->size();
    auto &bucket = extent_buckets->operator[](index);

    pthread_mutex_lock(&bucket.lock);
    auto itr = bucket.entries.find(extent_tag);
    if (itr != bucket.entries.end()) // Existing entry
    {
        if (wrlock)
        {
            assert(itr->second.refcount == -1);
            if (mark_clean)
            {
                itr->second.dirty = false;
            }
            itr->second.refcount++;
        }
        else
        {
            itr->second.refcount--;
        }
    }
    else // No existing entry
    {
        assert(false);
    }
    pthread_mutex_unlock(&bucket.lock);
}

/**
 * Answer whether the extent is dirty or not.
 *
 * @param extent_tag The tag of the extent
 * @return A boolean
 */
bool extent_dirty(uint64_t extent_tag)
{
    assert(extent_tag == (extent_tag & (~EXTENT_MASK)));

    auto index = extent_bucket_hash(extent_tag) % extent_buckets->size();
    auto &bucket = extent_buckets->operator[](index);
    auto itr = bucket.entries.find(extent_tag);

    assert(itr != bucket.entries.end());

    return itr->second.dirty;
}

/**
 * Answer whether the extent is clean or not.
 *
 * @param extent_Tag The tag of the extent
 * @return A boolean
 */
bool extent_clean(uint64_t extent_tag)
{
    return !(extent_dirty(extent_tag));
}

/**
 * Return the tag of the first dirty, unreferenced extent through the
 * pointer.
 *
 * @param extent_tag The return pointer
 * @return A boolean indicating whether an extent was found
 */
bool extent_first_dirty_unreferenced(uint64_t *extent_tag)
{
    for (size_t i = 0; i < EXTENT_BUCKETS; ++i)
    {
        auto j = (i + moop) % EXTENT_BUCKETS;
        auto &bucket = extent_buckets->operator[](j);

        pthread_mutex_lock(&bucket.lock);
        for (auto itr = bucket.entries.begin(); itr != bucket.entries.end(); ++itr)
        {
            if (itr->second.dirty && itr->second.refcount == 0)
            {
                uint64_t retval = itr->first;
                pthread_mutex_unlock(&bucket.lock);
                *extent_tag = retval;
                moop = j;
                return true;
            }
            else if (!itr->second.dirty && itr->second.refcount == 0)
            {
                // From
                // http://www.cplusplus.com/reference/map/map/erase/:
                // Iterators, pointers and references referring to
                // elements removed by the function are invalidated.
                // All other iterators, pointers and references keep
                // their validity.
                auto old_itr = itr;
                itr++;
                itr++;
                bucket.entries.erase(old_itr);
            }
        }
        pthread_mutex_unlock(&bucket.lock);
    }
    return false;
}
