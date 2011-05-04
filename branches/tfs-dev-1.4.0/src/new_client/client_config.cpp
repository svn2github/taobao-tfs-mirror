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
int64_t ClientConfig::gc_interval_ = 1200;  //20 min
int64_t ClientConfig::expired_time_ = 1 * 86400; // 1 days
int64_t ClientConfig::batch_time_out_ = 3000000; // 3 seconds
