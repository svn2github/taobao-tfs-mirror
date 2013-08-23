/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: encode.cpp 19 2012-05-04 14:48:09Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <string>
#include "common/internal.h"

using namespace tfs::common;

namespace tfs
{
  namespace clientv2
  {
    namespace coding
    {
      static const char SMALL_TFS_FILE_KEY_CHAR = 'T';
      static const char LARGE_TFS_FILE_KEY_CHAR = 'L';
      static const char SMALL_TFS_FILE_KEY_CHAR_V2 = 'S';
      static const char LARGE_TFS_FILE_KEY_CHAR_V2 = 'M';

      static const char enc_table[] = "XabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWYZ0123456789_.";
      static const char dec_table[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,63,0,52,53,54,55,56,57,58,59,60,61,0,0,0,0,0,0,0,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,0,50,51,0,0,0,0,62,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

      void encode(const char *input, char *output, const int32_t len)
      {
        assert(len % 3 == 0);
        if (input != NULL && output != NULL)
        {
          //char buffer[FILE_NAME_EXCEPT_SUFFIX_LEN];
          //xor_mask(input, FILE_NAME_EXCEPT_SUFFIX_LEN, buffer);

          int32_t i = 0;
          int32_t k = 0;
          uint32_t value = 0;
          for (i = 0; i < len; i += 3)
          {
            //value = ((buffer[i] << 16) & 0xff0000) + ((buffer[i + 1] << 8) & 0xff00) + (buffer[i + 2] & 0xff);
            value = ((input[i] << 16) & 0xff0000) + ((input[i + 1] << 8) & 0xff00) + (input[i + 2] & 0xff);
            output[k++] = enc_table[value >> 18];
            output[k++] = enc_table[(value >> 12) & 0x3f];
            output[k++] = enc_table[(value >> 6) & 0x3f];
            output[k++] = enc_table[value & 0x3f];
          }
        }
      }

      void decode(const char *input, char *output, const int32_t len)
      {
        assert(len % 4 == 0);
        if (input != NULL && output != NULL)
        {
          int32_t i = 0;
          int32_t k = 0;
          uint32_t value = 0;
          //char buffer[FILE_NAME_EXCEPT_SUFFIX_LEN];
          for (i = 0; i < len; i += 4)
          {
            value = (dec_table[input[i] & 0xff] << 18) + (dec_table[input[i + 1] & 0xff] << 12) +
              (dec_table[input[i + 2] & 0xff] << 6) + dec_table[input[i + 3] & 0xff];
            output[k++] = static_cast<char> ((value >> 16) & 0xff);
            output[k++] = static_cast<char> ((value >> 8) & 0xff);
            output[k++] = static_cast<char> (value & 0xff);
          }
          //xor_mask(buffer, FILE_NAME_EXCEPT_SUFFIX_LEN, output);
        }
      }
    }
  }
}
