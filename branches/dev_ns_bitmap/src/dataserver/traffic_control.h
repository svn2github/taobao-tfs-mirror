/*
 * (C) 2007-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   duanfei <duanfei@taobao.com>
 *      - initial release
 *
 */

#ifndef TFS_DATASERVER_TRAFFICE_CONTROL_H_
#define TFS_DATASERVER_TRAFFICE_CONTROL_H_

#include <Timer.h>
#include "common/internal.h"
#include "common/error_msg.h"
#include "common/array_helper.h"
#include "common/statistics.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    typedef enum RW_STAT_TYPE_
    {
      RW_STAT_TYPE_READ =  0,
      RW_STAT_TYPE_WRITE = 1,
      RW_STAT_TYPE_STAT = 2,
      RW_STAT_TYPE_UNLINK = 3
    }RW_STAT_TYPE;
    class TrafficControl
    {
      public:
        TrafficControl();
        virtual ~TrafficControl();
        int64_t get_last_rw_traffic_stat_time_us(const bool input);
        int64_t get_last_mr_traffic_stat_time_us(const bool input);
        int rw_stat(const int32_t type, const int32_t ret, const bool first, const int32_t bytes);
        void rw_traffic_stat(const bool input, const int32_t bytes);
        void mr_traffic_stat(const bool input, const int32_t bytes);
        bool rw_traffic_out_of_threshold(const bool input) const;
        bool mr_traffic_out_of_threshold(const bool input) const;
      private:
        static const int8_t MAX_TRAFFIC_STAT_COUNT = 2;
        DISALLOW_COPY_AND_ASSIGN(TrafficControl);
        int64_t last_rw_traffic_stat_time_us_[MAX_TRAFFIC_STAT_COUNT];
        int64_t last_mr_traffic_stat_time_us_[MAX_TRAFFIC_STAT_COUNT];
        int64_t rw_traffic_bytes_stat_[MAX_TRAFFIC_STAT_COUNT];
        int64_t mr_traffic_bytes_stat_[MAX_TRAFFIC_STAT_COUNT];
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/
#endif /* TFS_DATASERVER_TRAFFICE_CONTROL_H_*/

