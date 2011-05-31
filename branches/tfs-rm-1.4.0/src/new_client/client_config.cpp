/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software_; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   nayan <nayan@taobao.com>
 *      - initial release
 *
 */
#include "client_config.h"

using namespace tfs::client;
using namespace std;

string StatItem::client_access_stat_ = "client_access_stat";
string StatItem::read_success_ = "read_success";
string StatItem::read_fail_ = "read_fail";
string StatItem::stat_success_ = "stat_success";
string StatItem::stat_fail_ = "stat_fail";
string StatItem::write_success_ = "write_success";
string StatItem::write_fail_ = "write_fail";
string StatItem::unlink_success_ = "unlink_success";
string StatItem::unlink_fail_ = "unlink_fail";
string StatItem::client_cache_stat_ = "client_cache_stat";
string StatItem::cache_hit_ = "cache_hit";
string StatItem::cache_miss_ = "cache_miss";
string StatItem::remove_count_ = "remove_count";

int64_t ClientConfig::segment_size_ = 2 * 1024 * 1024;
int64_t ClientConfig::batch_count_ = 8;
int64_t ClientConfig::batch_size_ = ClientConfig::segment_size_ * ClientConfig::batch_count_;
int64_t ClientConfig::client_retry_count_ = 3;      // retry times to read or write
// interval unit: ms
int64_t ClientConfig::stat_interval_ = 60000;    // 1min
int64_t ClientConfig::gc_interval_ = 43200000;  // 12h
int64_t ClientConfig::expired_time_ = 86400000; // 1 days
int64_t ClientConfig::batch_timeout_ = 3000; // 3s wait several response timeout
int64_t ClientConfig::wait_timeout_ = 3000;  // 3s wait single response timeout
