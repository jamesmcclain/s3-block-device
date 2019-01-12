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

#ifndef __TIMED_STRING_HPP__
#define __TIMED_STRING_HPP__


class timed_string_t {

private:
  long _nanos;
  std::string _filename;

public:
  timed_string_t(): _nanos(0), _filename("") {}

  timed_string_t(long nanos, std::string filename): _nanos(nanos), _filename(filename) {}

  timed_string_t & operator+=(const timed_string_t rhs) {
    if (_nanos < rhs._nanos) {
      _nanos = rhs._nanos;
      _filename = rhs._filename;
    }
    return *this;
  }

  bool operator==(timed_string_t rhs) const {
    return (_nanos == rhs._nanos) && (_filename == rhs._filename);
  }

  long nanos() const {
    return _nanos;
  }

  const std::string && filename () const {
    return std::move(_filename);
  }
};


#endif
