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

#include <cstdlib>

#include <unistd.h>
#include <pthread.h>

#include <boost/compute/detail/lru_cache.hpp>

#include "constants.h"
#include "lru.h"

typedef boost::compute::detail::lru_cache<uint64_t, bool> lru_cache_t;

static lru_cache_t lru_cache{0};
static pthread_mutex_t lru_cache_lock;

static void *(*lru_flusher)(void *) = nullptr;

/**
 * Specialization of the lru_cache_t::evict method.
 */
template <>
void lru_cache_t::evict()
{
    uint64_t tag;

    // evict item from the end of most recently used list
    typename list_type::iterator i = --m_list.end();
    tag = *i;
    m_map.erase(*i);
    m_list.erase(i);

    lru_flusher(reinterpret_cast<void *>(tag));
}

/**
 * Initialize the cache.
 *
 * @param size The maximuim number of cache entries.
 */
void lru_init(void *(*f)(void *))
{
    size_t local_cache_megabytes = LOCAL_CACHE_DEFAULT_MEGABYTES;
    size_t local_cache_extents;
    const char *str;

    lru_flusher = f;
    if ((str = getenv(S3BD_LOCAL_CACHE_MEGABYTES)) != nullptr)
    {
        sscanf(str, "%lu", &local_cache_megabytes);
    }
    local_cache_extents = (local_cache_megabytes * (1 << 20)) / EXTENT_SIZE;
    lru_cache_lock = PTHREAD_MUTEX_INITIALIZER;
    lru_cache = lru_cache_t{local_cache_extents};
}

/**
 * Deinitialize the cache.
 */
void lru_deinit()
{
    lru_cache.clear();
}

/**
 * Report a page as being in use.
 *
 * @param page_tag The page to report
 */
void lru_report_page(uint64_t page_tag)
{
    uint64_t extent_tag = page_tag & (~EXTENT_MASK);

    pthread_mutex_lock(&lru_cache_lock);
    lru_cache.insert(extent_tag, true);
    pthread_mutex_unlock(&lru_cache_lock);
}
