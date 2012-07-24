/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: erasure_code.cpp 706 2012-07-19 15:30:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */

#include <stdio.h>
#include <stdlib.h>

#include "galois.h"
#include "jerasure.h"
#include "erasure_code.h"
#include "tbsys.h"
#include "common/error_msg.h"

using namespace std;
using namespace tfs::common;

namespace tfs
{
  namespace dataserver
  {
    const int ErasureCode::ws_ = 8;
    const int ErasureCode::ps_ = 128;

    ErasureCode::ErasureCode()
    {
      clear();
    }

    ErasureCode::~ErasureCode()
    {
      tbsys::gDelete(matrix_);
      tbsys::gDelete(de_matrix_);
    }

    int ErasureCode::config(const int dn, const int pn, int* erased)
    {
      int ret = 0;
      dn_ = dn;
      pn_ = pn;

      matrix_ = new (std::nothrow) int[dn_*pn_];
      if (NULL == matrix_)
      {
        ret = EXIT_NO_MEMORY;
      }
      else
      {
        for (int i = 0; i < pn_; i++)
        {
          for (int j = 0; j < dn_; j++)
          {
            matrix_[i*dn_+j] = galois_single_divide(1, i ^ (pn_ + j), ws_);
          }
        }
      }
      int* bitmatrix = jerasure_matrix_to_bitmatrix(dn_, pn_, ws_, matrix_);
      tbsys::gDelete(matrix_);
      matrix_ = bitmatrix;

      // if need decode, cache decode matrix
      // compute decode matrix is a costful work
      if (NULL != erased)
      {
        de_matrix_ = new (std::nothrow) int[dn_*dn_*ws_*ws_];
        if (NULL == de_matrix_)
        {
          ret = EXIT_NO_MEMORY;
        }
        else
        {
          int alive = 0;
          for (int i = 0; i < dn_ + pn_; i++)
          {
            erased_[i] = erased[i];
            if (0 == erased[i])
            {
              alive++;
            }
          }

          if (alive < dn_)
          {
            ret = EXIT_NO_ENOUGH_DATA;
            TBSYS_LOG(ERROR, "no enough alive data for decode, alive: %d, ret: %d", alive, ret);
          }
          else
          {
            if (jerasure_make_decoding_bitmatrix(dn_, pn_, ws_,
                matrix_, erased, de_matrix_, dm_ids_) < 0)
            {
              ret = EXIT_MATRIX_INVALID;
              TBSYS_LOG(ERROR, "can't make decoding bitmatrix, ret: %d", ret);
            }
          }
        }
      }

      return ret;
    }

    void ErasureCode::bind(char* data, const int index, const int size)
    {
      data_[index] = data;
      size_[index] = size;
    }

    void ErasureCode::bind(char** data, const int len, const int size)
    {
      if (NULL != data)
      {
        int n = len < dn_ + pn_ ? len: dn_ + pn_;
        for (int i = 0; i < n; i++)
        {
          data_[i] = data[i];
          size_[i] = size;
        }
      }
    }

    void ErasureCode::clear()
    {
      dn_ = -1;
      pn_ = -1;
      matrix_ = NULL;
      de_matrix_ = NULL;
      for (int i = 0; i < EC_DATA_MAX; i++)
      {
        data_[i] = NULL;
        size_[i] = -1;
        dm_ids_[i] = -1;
        erased_[i] = -1;
      }
    }

    int ErasureCode::encode(const int size)
    {
      int ret = TFS_SUCCESS;

      if (NULL == matrix_)
      {
        ret = EXIT_MATRIX_INVALID;
        TBSYS_LOG(ERROR, "matrix invalid, which is null.");
      }
      else if (0 != size % (ws_ * ps_))
      {
        ret = EXIT_SIZE_INVALID;
        TBSYS_LOG(ERROR, "size(%d) %% (ws*ps) != 0, invalid.", size);
      }
      else
      {
        for (int i = 0; i < dn_ + pn_; i++)
        {
          if (NULL == data_[i] || size_[i] < size)
          {
            ret = EXIT_DATA_INVALID;
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        jerasure_bitmatrix_encode(dn_, pn_, ws_, matrix_, data_, data_ + dn_, size, ps_);
      }

      return ret;
    }

    int ErasureCode::decode(const int size)
    {
      int ret = TFS_SUCCESS;

      if (NULL == de_matrix_)
      {
        ret = EXIT_MATRIX_INVALID;
        TBSYS_LOG(ERROR, "matrix invalid, which is null.");
      }
      else if (0 != size % (ws_ * ps_))
      {
        ret = EXIT_SIZE_INVALID;
        TBSYS_LOG(ERROR, "size(%d) %% (ws*ps) != 0, invalid.", size);
      }
      else
      {
        for (int i = 0; i < dn_ + pn_; i++)
        {
          if (NULL == data_[i] || size_[i] < size)
          {
            ret = EXIT_DATA_INVALID;
            break;
          }
        }
      }

      if (TFS_SUCCESS == ret)
      {
        // recovery data
        for (int i = 0; i < dn_; i++)
        {
          if (erased_[i])
          {
            jerasure_bitmatrix_dotprod(dn_, ws_, de_matrix_ + i * dn_ * ws_ * ws_,
                dm_ids_, i, data_, data_ + dn_, size, ps_);
          }
        }

        // recovery pairty
        for (int i = 0; i < pn_; i++)
        {
          if (erased_[dn_+i])
          {
            jerasure_bitmatrix_dotprod(dn_, ws_, matrix_ + i * dn_ * ws_ * ws_,
                NULL, dn_ + i, data_, data_ + dn_, size, ps_);
          }
        }
      }

      return ret;
    }
  }
}
