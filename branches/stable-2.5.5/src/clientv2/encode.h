/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: encode.h 5 2012-05-04 14:44:56Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */
#ifndef TFS_CLIENTV2_ENCODE_H_
#define TFS_CLIENTV2_ENCODE_H_

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

      static const char* KEY_MASK = "Taobao-inc";
      static const int32_t KEY_MASK_LEN = strlen(KEY_MASK);
      static const char enc_table[] = "0JoU8EaN3xf19hIS2d.6pZRFBYurMDGw7K5m4CyXsbQjg_vTOAkcHVtzqWilnLPe";
      static const char dec_table[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,18,0,0,11,16,8,36,34,19,32,4,12,0,0,0,0,0,0,0,49,24,37,29,5,23,30,52,14,1,33,61,28,7,48,62,42,22,15,47,3,53,57,39,25,21,0,0,0,0,45,0,6,41,51,17,63,10,44,13,58,43,50,59,35,60,2,20,56,27,40,54,26,46,31,9,38,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

      int xor_mask(const char* source, const int32_t len, char* target)
      {
        for (int32_t i = 0; i < len; i++)
        {
          target[i] = source[i] ^ KEY_MASK[i % KEY_MASK_LEN];
        }
        return TFS_SUCCESS;
      }

      void encode(const char *input, char *output, const int32_t len)
      {
        assert(len % 3 == 0);
        if (input != NULL && output != NULL)
        {
          const int32_t buffer_len = len;
          char buffer[buffer_len];
          xor_mask(input, buffer_len, buffer);

          int32_t i = 0;
          int32_t k = 0;
          uint32_t value = 0;
          for (i = 0; i < len; i += 3)
          {
            value = ((buffer[i] << 16) & 0xff0000) + ((buffer[i + 1] << 8) & 0xff00) + (buffer[i + 2] & 0xff);
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
          const int32_t buffer_len = len * 3 / 4;
          char buffer[buffer_len];
          for (i = 0; i < len; i += 4)
          {
            value = (dec_table[input[i] & 0xff] << 18) + (dec_table[input[i + 1] & 0xff] << 12) +
              (dec_table[input[i + 2] & 0xff] << 6) + dec_table[input[i + 3] & 0xff];
            buffer[k++] = static_cast<char> ((value >> 16) & 0xff);
            buffer[k++] = static_cast<char> ((value >> 8) & 0xff);
            buffer[k++] = static_cast<char> (value & 0xff);
          }
          xor_mask(buffer, buffer_len, output);
        }
      }
    }
  }
}
#endif
