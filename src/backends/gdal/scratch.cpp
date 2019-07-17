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

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#include <vector>

#include "constants.h"
#include "scratch.h"

typedef struct
{
    pthread_mutex_t lock;
    int fd;
} locked_fd_t;

typedef std::vector<locked_fd_t> locked_fd_vector_t;

static locked_fd_vector_t *locked_fd_vector = nullptr;

/**
 * Initialize scratch file functionality.
 */
void scratch_init()
{
    char *scratch_dir = nullptr;
    char scratch_filename[0x100];

    // Initialize file descriptor list
    scratch_dir = getenv(S3BD_SCRATCH_DIR);
    if (scratch_dir != nullptr)
    {
        sprintf(scratch_filename, SCRATCH_TEMPLATE, scratch_dir, getpid());
    }
    else
    {
        sprintf(scratch_filename, SCRATCH_TEMPLATE, SCRATCH_DEFAULT_DIR, getpid());
    }
    if (locked_fd_vector == nullptr)
    {
        locked_fd_vector = new locked_fd_vector_t{};
        for (size_t i = 0; i < SCRATCH_DESCRIPTORS; ++i)
        {
            locked_fd_vector->push_back(locked_fd_t{
                PTHREAD_MUTEX_INITIALIZER,
                open(scratch_filename, O_RDWR | O_CREAT, S_IRWXU)});
        }
    }

    // Unlink scratch file if not told to keep it
    if (getenv(S3BD_KEEP_SCRATCH_FILE) == nullptr)
    {
        unlink(scratch_filename);
    }
}

/**
 * Deinitialize scratch file functionaltiy.
 */
void scratch_deinit()
{
    if (locked_fd_vector != nullptr)
    {
        for (size_t i = 0; i < SCRATCH_DESCRIPTORS; ++i)
        {
            close(locked_fd_vector->operator[](i).fd);
        }
        delete locked_fd_vector;
        locked_fd_vector = nullptr;
    }
}

/**
 * Acquire a handle to a file descriptor for the scratch file.
 *
 * @return A handle that is mappable to a file descriptor.
 */
size_t aquire_scratch_handle()
{
    for (size_t i = 0; true; ++i)
    {
        size_t handle = i % SCRATCH_DESCRIPTORS;
        pthread_mutex_t *lock = &(locked_fd_vector->operator[](handle).lock);

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
int scratch_handle_to_fd(size_t handle)
{
    return locked_fd_vector->operator[](handle).fd;
}

/**
 * Release a scratch file handle.
 *
 * @param handle The handle to release
 */
void release_scratch_handle(size_t handle)
{
    pthread_mutex_t *lock = &(locked_fd_vector->operator[](handle).lock);
    pthread_mutex_unlock(lock);
}
