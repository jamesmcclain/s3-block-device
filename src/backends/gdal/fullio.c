/*
 * The MIT License
 *
 * Copyright (c) 2017-2019 James McClain
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

#include <stdint.h>
#include <unistd.h>
#include "fullio.h"

#if 1
void fullwrite(int fd, const void *buffer, int bytes)
{
  int sent = 0;

  while (bytes - sent > 0)
  {
    int i = write(fd, buffer + sent, bytes - sent);
    if (i < 0)
      break;
    sent += i;
  }
}

void fullread(int fd, void *buffer, int bytes)
{
  int recvd = 0, i = 0;

  while (1)
  {
    i = read(fd, buffer + recvd, bytes - recvd);
    if (i < 0)
      break;
    if ((i <= bytes - recvd) || (recvd == bytes))
      break;
    recvd += i;
  }
}
#else
void fullwrite(int fd, const void *buffer, int bytes)
{
  write(fd, buffer, bytes);
}

void fullread(int fd, void *buffer, int bytes)
{
  read(fd, buffer, bytes);
}
#endif