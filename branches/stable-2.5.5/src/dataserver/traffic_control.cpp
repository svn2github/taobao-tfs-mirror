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

#include "common/internal.h"
#include "common/error_msg.h"
#include "common/array_helper.h"
#include "ds_define.h"
#include "traffic_control.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    using namespace std;
    using namespace tfs::common;
    TrafficControl::TrafficControl()
    {
      memset(last_rw_traffic_stat_time_us_, 0, sizeof(last_rw_traffic_stat_time_us_));
      memset(last_mr_traffic_stat_time_us_, 0, sizeof(last_mr_traffic_stat_time_us_));
      memset(rw_traffic_bytes_stat_, 0, sizeof(rw_traffic_bytes_stat_));
      memset(mr_traffic_bytes_stat_, 0, sizeof(mr_traffic_bytes_stat_));
    }

    TrafficControl::~TrafficControl()
    {

    }

    int64_t TrafficControl::get_last_rw_traffic_stat_time_us(const bool input)
    {
      int index = input ? 0 : 1;
      return last_rw_traffic_stat_time_us_[index];
    }

    int64_t TrafficControl::get_last_mr_traffic_stat_time_us(const bool input)
    {
      int index = input ? 0 : 1;
      return last_mr_traffic_stat_time_us_[index];
    }

    int TrafficControl::rw_stat(const int32_t type,const int32_t ret, const bool first, const int32_t bytes)
    {
      int32_t result = (type >= RW_STAT_TYPE_READ && type <= RW_STAT_TYPE_UNLINK) ? TFS_SUCCESS : EXIT_PARAMETER_ERROR;
      if (TFS_SUCCESS == result)
      {
        DataServerStatInfo& info = DsRuntimeGlobalInformation::instance().information_;
        int8_t index = TFS_SUCCESS == ret ? 0 : 1;
        switch(type)
        {
        case RW_STAT_TYPE_READ:
          atomic_add(reinterpret_cast<uint64_t*>(&info.read_bytes_[index]), bytes);
          if (first)
            atomic_add(reinterpret_cast<uint64_t*>(&info.read_file_count_[index]), 1);
        break;
        case RW_STAT_TYPE_WRITE:
          atomic_add(reinterpret_cast<uint64_t*>(&info.write_bytes_[index]), bytes);
          if (first)
            atomic_add(reinterpret_cast<uint64_t*>(&info.write_file_count_[index]), 1);
        break;
        case RW_STAT_TYPE_STAT:
          if (first)
            atomic_add(reinterpret_cast<uint64_t*>(&info.stat_file_count_[index]), 1);
        break;
        case RW_STAT_TYPE_UNLINK:
          if (first)
            atomic_add(reinterpret_cast<uint64_t*>(&info.unlink_file_count_[index]), 1);
        break;
        default:
          break;
        };
      }
      return result;
    }

    void TrafficControl::rw_traffic_stat(const bool input, const int32_t bytes)
    {
      int8_t index = input ? 0 : 1;
      int64_t now = Func::get_monotonic_time_us();
      if (now - last_rw_traffic_stat_time_us_[index]>= TRAFFIC_BYTES_STAT_INTERVAL)
      {
        atomic_exchange(reinterpret_cast<uint64_t*>(&last_rw_traffic_stat_time_us_[index]), now);
        atomic_exchange(reinterpret_cast<uint64_t*>(&rw_traffic_bytes_stat_[index]), 0);
      }
      atomic_add(reinterpret_cast<uint64_t*>(&rw_traffic_bytes_stat_[index]), bytes);
    }

    void TrafficControl::mr_traffic_stat(const bool input, const int32_t bytes)
    {
      int8_t index = input ? 0 : 1;
      int64_t now = Func::get_monotonic_time_us();
      if (now - last_mr_traffic_stat_time_us_[index] >= TRAFFIC_BYTES_STAT_INTERVAL)
      {
        atomic_exchange(reinterpret_cast<uint64_t*>(&last_mr_traffic_stat_time_us_[index]), now);
        atomic_exchange(reinterpret_cast<uint64_t*>(&mr_traffic_bytes_stat_[index]), 0);
      }
      atomic_add(reinterpret_cast<uint64_t*>(&mr_traffic_bytes_stat_[index]), bytes);
    }

    bool TrafficControl::rw_traffic_out_of_threshold(const bool input) const
    {
      int8_t index = input ? 0 : 1;
      int64_t now = Func::get_monotonic_time_us();
      return ((now - last_rw_traffic_stat_time_us_[index]) < TRAFFIC_BYTES_STAT_INTERVAL)
              && (rw_traffic_bytes_stat_[index] >= (DsRuntimeGlobalInformation::instance().max_rw_network_bandwidth_mb_ * MB));
    }

    bool TrafficControl::mr_traffic_out_of_threshold(const bool input) const
    {
      int8_t index = input ? 0 : 1;
      int64_t now = Func::get_monotonic_time_us();
      return ((now - last_mr_traffic_stat_time_us_[index]) < TRAFFIC_BYTES_STAT_INTERVAL)
              && (mr_traffic_bytes_stat_[index] >= (DsRuntimeGlobalInformation::instance().max_mr_network_bandwidth_mb_ * MB));
    }
  }/** end namespace dataserver **/
}/** end namespace tfs **/
