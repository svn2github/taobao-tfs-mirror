/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * easybuffer, wrapper of easy_buf_t
 *
 * Authors:
 *   gy <ganyu.hfl@taobao.com>
 *     - add easy
 *
 */
#ifndef TFS_COMMON_EASY_BUFFER_H
#define TFS_COMMON_EASY_BUFFER_H

#include <byteswap.h>
#include <easy_buf.h>
#include <easy_io.h>
#include <string>

#define TAIR_PACKET_HEADER_SIZE 16
namespace tfs
{
namespace common
{
class EasyBuffer
{
public:
  EasyBuffer()
  {
    l = NULL;
    mark_end = NULL;
    pool = easy_pool_create(0);
    b = easy_buf_create(pool, 0);
    alloc = true;
  }

  EasyBuffer(const int64_t length)
  {
    l = NULL;
    mark_end = NULL;
    pool = easy_pool_create(0);
    b = easy_buf_create(pool, length);
    alloc = true;
  }


  EasyBuffer(easy_buf_t *pb)
  {
    pool = NULL;
    l = NULL;
    b = pb;
    mark_end = NULL;
    alloc = false;
  }

  EasyBuffer(easy_pool_t *p, easy_list_t *pl, uint32_t size = 0)
  {
    pool = p;
    l = pl;
    //~ pre-allocate space for `this->b',
    //~ (b->last - b->pos) would be aligned at EASY_POOL_PAGE_SIZE
    b = (size == 0) ? NULL : easy_buf_check_write_space(pool, l, size);
    mark_end = NULL;
    alloc = false;
  }

  ~EasyBuffer()
  {
    if (alloc)
    {
      easy_buf_destroy(b);
      easy_pool_destroy(pool);
    }
  }

  void reserve(size_t size)
  {
    if (pool != NULL && l != NULL)
    {
      b = easy_buf_check_write_space(pool, l, size);
    }
  }

  void clear()
  {
    b->last = b->pos;
  }

  char* get_data() const
  {
    return b->pos;
  }

  int64_t get_data_length() const
  {
    if (NULL != mark_end)
    {
      return mark_end - b->pos;
    }
    return b->last - b->pos;
  }

  char* get_free() const
  {
    return b->last;
  }

  int64_t get_free_length() const
  {
    return b->end - b->last;
  }

  int64_t get_buf_length() const
  {
    return b->end - b->pos;
  }

  void drain(const int64_t length)
  {
    char* end = (NULL != mark_end) ? mark_end : b->last;
    int64_t len = std::min(length, end - b->pos);
    assert(b->last - b->pos >= len);
    b->pos += len;
  }

  void pour(const int64_t length)
  {
    assert(b->end - b->last >= length);
    b->last += length;
  }

  void expand(const int64_t need)
  {
    return reserve(need);
  }

  // use for reading EasyBuffer
  void set_last_read_mark(uint32_t len)
  {
    assert(b->pos + len <= b->last);
    mark_end = b->pos + len;
  }

  void clear_last_read_mark()
  {
    mark_end = NULL;
  }

  /*

  int getDataLen() {
    return (b->last - b->pos);
  }

  void drainData(int len) {
    b->last += len;
  }

  void writeInt8(uint8_t n) {
    b = easy_buf_check_write_space(pool, l, 1);
    *b->last++ = (unsigned char)n;
  }

  void writeInt16(uint16_t n) {
    b =  easy_buf_check_write_space(pool, l, 2);
    *((uint16_t *)b->last) = bswap_16(n);
    b->last += 2;
  }

  void writeInt32(uint32_t n) {
    b = easy_buf_check_write_space(pool, l, 4);
    *((uint32_t *)b->last) = bswap_32(n);
    b->last += 4;
  }

  void writeInt64(int64_t n) {
    b = easy_buf_check_write_space(pool, l, 8);
    *((int64_t *)b->last) = bswap_64(n);
    b->last += 8;
  }

  void writeInt64(uint64_t n) {
    b = easy_buf_check_write_space(pool, l, 8);
    *((uint64_t *)b->last) = bswap_64(n);
    b->last += 8;
  }

  void writeBytes(const void *src, int len) {
    //if(len>65536)
    //{
      //log_warn( "buffer data too longggg,%d",len);
    //}
    b = easy_buf_check_write_space(pool, l, len);
    memcpy(b->last, src, len);
    b->last += len;
  }

  void fillInt8(unsigned char *dst, uint8_t n) {
    *dst = n;
  }

  void fillInt16(unsigned char *dst, uint16_t n) {
    *((uint16_t*)dst) = bswap_16(n);
  }

  void fillInt32(unsigned char *dst, uint32_t n) {
    *((uint32_t*)dst) = bswap_32(n);
  }

  void fillInt64(unsigned char *dst, uint64_t n) {
    *((uint64_t*)dst) = bswap_64(n);
  }

  void writeString(const char *str) {
    int len = (str ? strlen(str) : 0);
    if (len > 0) len ++;
    b = easy_buf_check_write_space(pool, l, len + sizeof(uint32_t));
    writeInt32(len);
    if (len > 0) {
      memcpy(b->last, str, len);
      b->last += (len);
    }
  }

  bool writeLittleString(const std::string &str)
  {
    if (str.size() < 0 || str.size() > 1024) return false;
    int16_t len = (int16_t)str.size();
    writeInt16(len);
    b = easy_buf_check_write_space(pool, l, len);
    memcpy(b->last, str.c_str(), len);
    b->last += len;
    return true;
  }

  void writeString(const std::string &str) {
    writeString(str.c_str());
  }

  uint8_t readInt8() {
    return ((*b->pos++) & 0xff);
  }

  bool readInt8(uint8_t* number) {
    assert(number != NULL);
    if (b->pos + 1 > mark_end) {
      return false;
    }
    *number = readInt8();
    return true;
  }

  uint16_t readInt16() {
    uint16_t n = bswap_16(*((uint16_t *)b->pos));
    b->pos += 2;
    return n;
  }

  bool readInt16(uint16_t* number) {
    assert(number != NULL);
    if (b->pos + 2 > mark_end) {
      return false;
    }
    *number = readInt16();
    return true;
  }

  uint32_t readInt32() {
    uint32_t n = bswap_32(*((uint32_t *)b->pos));
    b->pos += 4;
    assert(b->pos <= b->last);
    return n;
  }

  bool readInt32(uint32_t* number) {
    assert(number != NULL);
    if (b->pos + 4 > mark_end) {
      return false;
    }
    *number = readInt32();
    return true;
  }

  bool readInt16(int16_t* n)
  {
    if (b->pos + 2 > mark_end) {
      return false;
    }
    *n = bswap_16(*(int16_t *)b->pos);
    b->pos += 2;
    return true;
  }

  bool readInt32(int32_t* n)
  {
    if (b->pos + 4 > mark_end) {
      return false;
    }
    *n = bswap_32(*(int32_t *)b->pos);
    b->pos += 4;
    return true;
  }

  bool readInt64(int64_t* n)
  {
    if (b->pos + 8 > mark_end) {
      return false;
    }
    *n = bswap_64(*(int64_t *)b->pos);
    b->pos += 8;
    return true;
  }

  uint64_t readInt64() {
    uint64_t n = bswap_64(*((uint64_t *)b->pos));
    b->pos += 8;
    assert(b->pos <= b->last);
    return n;
  }

  bool readInt64(uint64_t* number) {
    assert(number != NULL);
    if (b->pos + 8 > mark_end) {
      return false;
    }
    *number = readInt64();
    return true;
  }

  bool readBytes(void *dst, int len) {
    if (b->pos + len > mark_end || b->pos + len > b->last) {
      return false;
    }

    memcpy(dst, b->pos, len);
    b->pos += len;
    assert(b->pos <= mark_end);
    assert(b->pos <= b->last);
    return true;
  }

  bool readLittleString(std::string &str)
  {
    if (b->pos + 2 > mark_end)
      return false;
    int16_t size = 0;
    readInt16(&size);
    if (size < 0 || size > 1024) return false;
    if (b->pos + size > mark_end)
    {
      return false;
    }
    str.assign(b->pos, size);
    b->pos += size;
    return true;
  }

  bool readString(char *&str, int len) {
    if (b->pos + sizeof(int) > mark_end ||
        b->pos + sizeof(int) > b->last) {
      return false;
    }
    uint32_t slen = readInt32();
    if (b->last - b->pos < slen || mark_end < b->pos + slen) {
      // read string error, we should return false, not continue to do remaining work
      return false;
    }
    if (str == NULL && slen > 0) {
      str = (char*)malloc(slen);
      len = slen;
    }
    if ((uint32_t)len > slen) {
      len = slen;
    }
    if (len > 0) {
      memcpy(str, b->pos, len);
      str[len - 1] = '\0';
    }
    b->pos += slen;
    assert(b->pos <= b->last);
    assert(b->pos <= mark_end);
    return true;
  }

  */
private:
  easy_pool_t         *pool;
  easy_list_t         *l;
  easy_buf_t          *b;
  int                 wl;
  char                *mark_end;
  bool alloc;
};
}
}

#endif
