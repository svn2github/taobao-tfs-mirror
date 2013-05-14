/** (C) 2007-2013 Alibaba Group Holding Limited.
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

#ifndef TFS_DATASERVER_BLOCK_MANAGER_H_
#define TFS_DATASERVER_BLOCK_MANAGER_H_

#include "common/lock.h"
#include "common/internal.h"
#include "common/parameter.h"

#include "ds_define.h"
#include "gc.h"
#include "logic_blockv2.h"
#include "super_block_manager.h"
#include "logic_block_manager.h"
#include "super_block_manager.h"
#include "physical_block_manager.h"

#ifdef TFS_GTEST
#include <gtest/gtest.h>
#endif

namespace tfs
{
  namespace dataserver
  {
    class BlockManager
    {
        #ifdef TFS_GTEST
          friend class TestBlockManager;
          FRIEND_TEST(TestBlockManager, format);
          FRIEND_TEST(TestBlockManager, bootstrap);
        #endif
        friend class BaseLogicBlock;
      public:
        explicit BlockManager(const std::string& super_block_path);
        virtual ~BlockManager();

        int format(const common::FileSystemParameter& parameter);
        int cleanup(const common::FileSystemParameter& parameter);
        int bootstrap(const common::FileSystemParameter& parameter);

        int new_block(const uint64_t logic_block_id, const bool tmp = false, const int64_t family_id = common::INVALID_FAMILY_ID, const int8_t index_num = 0, const int32_t expire_time = DEFAULT_BLOCK_EXPIRE_TIME);
        int del_block(const uint64_t logic_block_id, const bool tmp = false);
        BaseLogicBlock* get(const uint64_t logic_block_id, const bool tmp = false) const;
        int get_blocks_in_time_range(const common::TimeRange& range, std::vector<uint64_t>& blocks) const;
        int get_all_block_ids(std::vector<uint64_t>& blocks) const;
        int get_all_block_info(std::set<common::BlockInfo>& blocks) const;
        int get_all_block_info(std::vector<common::BlockInfoV2>& blocks) const;
        int get_all_block_info(common::ArrayHelper<common::BlockInfoV2>& blocks) const;
        int get_all_logic_block_to_physical_block(std::map<uint64_t, std::vector<int32_t> >& blocks) const;
        int32_t get_all_logic_block_count() const;
        int get_space(int64_t& total_space, int64_t& used_space) const;

        int switch_logic_block(const uint64_t logic_block_id, const bool tmp = false);
        int timeout(const time_t now);

        int get_family_id(int64_t& family_id, const uint64_t logic_block_id) const;
        int set_family_id(const int64_t family_id, const uint64_t logic_block_id);
        int get_used_offset(int32_t& size, const uint64_t logic_block_id) const;
        int set_used_offset(const int32_t size, const uint64_t logic_block_id);
        int get_marshalling_offset(int32_t& size, const uint64_t logic_block_id) const;
        int set_marshalling_offset(const int32_t size, const uint64_t logic_block_id);

        int get_index_header(common::IndexHeaderV2& header, const uint64_t logic_block_id) const;
        int set_index_header(const common::IndexHeaderV2& header, const uint64_t logic_block_id, const bool tmp = false);

        int check_block_version(common::BlockInfoV2& info, const int32_t remote_version,
              const uint64_t logic_block_id, const uint64_t attach_logic_block_id) const;
        int update_block_info(const common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID) const;
        int update_block_version(const int8_t step = common::VERSION_INC_STEP_DEFAULT, const uint64_t logic_block_id = common::INVALID_BLOCK_ID, const bool tmp = false);
        int get_block_info(common::BlockInfoV2& info, const uint64_t logic_block_id = common::INVALID_BLOCK_ID, const bool tmp = false) const;

        int generation_file_id(uint64_t& fileid, const uint64_t logic_block_id);
        int pwrite(const char* buf, const int32_t nbytes, const int32_t offset, const uint64_t logic_block_id, const bool tmp = false);
        int pread(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t logic_block_id);
        int write(uint64_t& fileid, DataFile& datafile, const uint64_t logic_block_id,
            const uint64_t attach_logic_block_id, const bool tmp = false);
        int read(char* buf, int32_t& nbytes, const int32_t offset, const uint64_t fileid,
            const int8_t flag, const uint64_t logic_block_id, const uint64_t attach_logic_block_id);
        int stat(common::FileInfoV2& info, const int8_t flag, const uint64_t logic_block_id, const uint64_t attach_logic_block_id) const;
        int unlink(int64_t& size, const uint64_t fileid, const int32_t action,
              const uint64_t logic_block_id, const uint64_t attach_logic_block_id);

        int write_file_infos(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& infos, const uint64_t logic_block_id, uint64_t attach_logic_block_id, const bool tmp = false, const bool partial = false);
        int traverse(common::IndexHeaderV2& header, std::vector<common::FileInfoV2>& finfos, const uint64_t logic_block_id, uint64_t attach_logic_block_id) const;
        int get_attach_blocks(common::ArrayHelper<uint64_t>& blocks, const uint64_t logic_block_id) const;
        int get_index_num(int32_t& index_num, const uint64_t logic_block_id) const;

        bool exist(const uint64_t logic_block_id, const bool tmp = false) const;

        SuperBlockManager& get_super_block_manager() { return super_block_manager_;}
        LogicBlockManager& get_logic_block_manager() { return logic_block_manager_;}
        PhysicalBlockManager& get_physical_block_manager() { return physical_block_manager_;}
        GCObjectManager& get_gc_manager() { return gc_manager_;}

      private:
        int load_super_block_(const common::FileSystemParameter& fs_param);
        int load_index_(const common::FileSystemParameter& fs_param);
        int cleanup_dirty_index_(const common::FileSystemParameter& fs_param);
        int cleanup_dirty_index_single_logic_block_(const BlockIndex& index);

        int create_file_system_superblock_(const common::FileSystemParameter& parameter);
        int create_file_system_dir_(const common::FileSystemParameter& parameter);
        int fallocate_block_(const common::FileSystemParameter& parameter);

        BasePhysicalBlock* insert_physical_block_(const SuperBlockInfo& info, const BlockIndex& index, const int32_t physical_block_id, const std::string& path);
        BaseLogicBlock* insert_logic_block_(const uint64_t logic_block_id, const std::string& index_path, const bool tmp = false, const int32_t expire_time = DEFAULT_BLOCK_EXPIRE_TIME);
        BaseLogicBlock* get_(const uint64_t logic_block_id, const bool tmp = false) const;
        bool exist_(const uint64_t logic_block_id, const bool tmp = false) const;
        int del_block_(const uint64_t logic_block_id, const bool tmp = false);

      private:
        SuperBlockManager super_block_manager_;
        LogicBlockManager logic_block_manager_;
        PhysicalBlockManager physical_block_manager_;
        GCObjectManager   gc_manager_;
        mutable common::RWLock mutex_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* TFS_DATASERVER_BLOCK_MANAGER_H_ */
