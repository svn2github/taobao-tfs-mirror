/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: erasure_code.h 706 2012-07-19 15:30:41Z linqing.zyd@taobao.com $
 *
 * Authors:
 *   linqing <linqing.zyd@taobao.com>
 *      - initial release
 *
 */


#include "dataserver_define.h"

namespace tfs
{
  namespace dataserver
  {
    class ErasureCode
    {
      public:

        /**
         * @brief constructor
         */
        ErasureCode();

        /**
         * @brief destructor
         */
        virtual ~ErasureCode();

        /**
        * @brief config erasure code
        *
        * @param dn: data numbers
        * @param pn: parity numbers
        * @param erased: which disk is erased
        *        only used on decode, lenth: dn + pn
        * @return 0 on succss
        */
        int config(const int dn, const int pn, int* erased = NULL);

        /**
         * @brief bind data with encode/decode buffer
         *
         * @param data: data buffer
         * @param index: the data index
         * @param size: data size
         */
        void bind(char* data, const int index, const int size);

        /**
         * @brief bind data
         *
         * @param data: data pointer array
         * @param len: pointer array size
         * @param size: data size
         */
        void bind(char** data, const int len, const int size);

        /**
         * @brief clear
         */
        void clear();

        /**
         * @brief encode data specified by size
         * should implemented by specific algorithm
         *
         * @param size: size to encode
         *
         * @return 0 on success
         */
        int encode(const int size);

        /**
         * @brief decode data specified by size
         *
         * should implemented by specific algorithm
         *
         * @param size
         *
         * @return
         */
        int decode(const int size);

      public:
        static const int ws_;         // word size, shouldn't change
        static const int ps_;         // packet_size, shouldn't change

      private:
        DISALLOW_COPY_AND_ASSIGN(ErasureCode);

       int dn_;                      // data numbers
        int pn_;                      // parity numbers
        int* matrix_;                 // encode matrix
        int* de_matrix_;              // decode matrix
        char* data_[EC_DATA_MAX];     // data binding
        int size_[EC_DATA_MAX];       // size of each stripe
        int dm_ids_[EC_DATA_MAX];     // alive disks
        int erased_[EC_DATA_MAX];     // erased disk byte map
    };
  }
}



