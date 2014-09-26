#include "stat_tool.h"

using namespace std;
using namespace tfs::common;
using namespace tfs::tools;

ValueRange::ValueRange(const int32_t min_value, const int32_t max_value)
  : min_value_(min_value), max_value_(max_value), count_(0)
{
}
ValueRange::~ValueRange()
{
  count_ = 0;
}
bool ValueRange::is_in_range(const int32_t value) const
{
  bool ret = false;
  if (max_value_ == -1)
  {
    if (value >= min_value_)
    {
      ret = true;
    }
  }
  else if (min_value_ <= max_value_)
  {
    if ((value >= min_value_) && (value < max_value_))
    {
      ret = true;
    }
  }
  return ret;
}

void ValueRange::incr()
{
  count_++;
}

void ValueRange::decr()
{
  count_--;
}

BlockSizeRange::BlockSizeRange(const int32_t min_value, const int32_t max_value)
  :ValueRange(min_value, max_value)
{
}

BlockSizeRange::~BlockSizeRange()
{
}

void BlockSizeRange::dump(FILE* fp) const
{
  if (max_value_ != -1)
  {
    fprintf(fp, "%dM ~ %dM: %"PRI64_PREFIX"d\n", min_value_, max_value_, count_);
  }
  else
  {
    fprintf(fp, "%dM ~ : %"PRI64_PREFIX"d\n", min_value_, count_);
  }
}

DelBlockRange::DelBlockRange(const int32_t min_value, const int32_t max_value)
  : ValueRange(min_value, max_value)
{
}

DelBlockRange::~DelBlockRange()
{
  count_ = 0;
}

void DelBlockRange::dump(FILE* fp) const
{
  if (max_value_ != -1)
  {
    fprintf(fp, "%d%% ~ %d%%: %"PRI64_PREFIX"d\n", min_value_, max_value_, count_);
  }
  else
  {
    fprintf(fp, "%d%% ~ : %"PRI64_PREFIX"d\n", min_value_, count_);
  }
}


// stat info stat
StatInfo::StatInfo()
  : block_count_(0), file_count_(0), file_size_(0), del_file_count_(0), del_file_size_(0),
    replicate_count_(0), data_block_count_(0), family_count_(0), total_file_size_(0)
{
}
StatInfo::~StatInfo()
{
}
void StatInfo::set_family_count(const int64_t family_count)
{
  family_count_ = family_count;
}

float StatInfo::div(const int64_t a, const int64_t b)
{
  float ret = 0;
  if (b > 0)
  {
    ret = static_cast<float>(a) / b;
  }
  return ret;
}

void StatInfo::add(const BlockBase& block_base, const float ratio)
{
  block_count_++;
  file_count_ += block_base.info_.file_count_;
  file_size_ += block_base.info_.size_;
  del_file_count_ += block_base.info_.del_file_count_;
  del_file_size_ += block_base.info_.del_size_;
  replicate_count_ += block_base.server_list_.size();
  if (!IS_VERFIFY_BLOCK(block_base.info_.block_id_))
  {
    data_block_count_ += 1;
  }
  total_file_size_ += static_cast<int>((block_base.info_.size_ - block_base.info_.del_size_) * ratio);
}

void StatInfo::dump(FILE* fp) const
{
  fprintf(fp, "user_file_count: %"PRI64_PREFIX"d, user_file_size: %"PRI64_PREFIX"d, avg_file_size: %.2f,"
      " del_file_count: %"PRI64_PREFIX"d, del_file_size: %"PRI64_PREFIX"d, del_avg_file_size: %.2f, del_ratio: %.2f%%\n",
      file_count_, file_size_, div(file_size_, file_count_),
      del_file_count_, del_file_size_, div(del_file_size_, del_file_count_), div(del_file_size_ * 100, file_size_));
  fprintf(fp, "block_count: %"PRI64_PREFIX"d, avg_block_size: %.2f, family_count: %"PRI64_PREFIX"d, "
      "replicates: %"PRI64_PREFIX"d, data_blocks: %"PRI64_PREFIX"d, avg_replicate_count: %.3f, total_file_size: %"PRI64_PREFIX"d\n",
      block_count_, div(file_size_, block_count_), family_count_,
      replicate_count_, data_block_count_, div(replicate_count_, data_block_count_), total_file_size_);
}
