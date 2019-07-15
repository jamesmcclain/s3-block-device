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

#include <cstdio>
#include <cstdlib>

#include <unistd.h>
#include <pthread.h>

#include "constants.h"
#include "sync.h"

static pthread_t sync_thread;
static pthread_t unqueue_thread;
bool sync_thread_continue = false;

/**
 * Initialize the syncing threads.
 *
 * @param size The maximuim number of cache entries.
 */
void sync_init(void *(*f)(void *), void *(*g)(void *))
{
    sync_thread_continue = true;
    pthread_create(&sync_thread, NULL, f, nullptr);
    pthread_create(&unqueue_thread, NULL, g, nullptr);
}

/**
 * Deinitialize the syncing threads.
 */
void sync_deinit()
{
    sync_thread_continue = false;
    pthread_join(sync_thread, nullptr);
    pthread_join(unqueue_thread, nullptr);
}
