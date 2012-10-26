/* * (C) 2007-2013 Alibaba Group Holding Limited.
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

namespace tfs
{
  namespace dataserver
  {
    class BlockManager
    {
      public:
        BlockManager();
        virtual ~BlockManager();

        //static BlockManager& instance() { static BlockManager instance_; return instance_;}

        int format(const common::FileSystemParameter& parameter);
        int cleanup(FileSystemParameter& parameter);
        int bootstrap(const common::FileSystemParameter& parameter);

        int new_block(const uint64_t logic_block_id);
        int new_ext_block(const uint64_t logic_block_id);
        int del_block(const uint64_t logic_block_id);
        LogicBlock* get(const uint64_t logic_block_id);
        int get_all_block_info(std::set<BlockInfo>& blocks) const;
        int get_all_block_info(std::set<common::BlockInfoExt>& blocks) const;
        int get_all_logic_block_to_physical_block(std::map<uint64, std::vector<uint32_t> >& blocks) const;
        int get_all_block_id(std::vector<BlockInfoV2>& blocks) const;
        int get_all_logic_block_count() const;
        int get_space(int64_t& total_space, int64_t& used_space) const;
        int get_superblock_info(common::SuperBlockInfo& info) const;

        int switch_block_from_tmp(const uint64_t logic_block_id);
        int timeout(const time_t now);

        inline SuperBlockManager& get_super_block_manager() { return super_block_manager_;}
        inline LogicBlockManager& get_logic_block_manager() { return logic_block_manager_;}
        inline PhysicalBlockManager& get_physical_block_manager() { return physical_block_manager_;}

      private:
        int load_supber_block_(const common::FileSystemParameter& fs_param);
        int load_index_(const common::FileSystemParameter& fs_param);
        int cleanup_dirty_index_(const common::FileSystemParameter& fs_param);

        int create_file_system_superblock_(const FileSystemParameter& parameter);
        int create_file_system_dir_(const FileSystemParameter& parameter);
        int fallocate_block_(const FileSystemParameter& parameter);

        int get_avail_physical_block_id_(uint32_t& physical_block_id);
        int del_logic_block_from_table_(const uint64_t logic_block_id);
        int del_logic_block_from_tmp_(const uint64_t logic_block_id);

        int rollback_superblock_(const uint32_t physical_block_id, const int8_t type, const bool modify = false);

        BasePhysicalBlock* insert_physical_block_(const SupberBlockInfo& info, const BlockIndex& index, const int32_t physical_block_id, const std::string& path);
        LogicBlock* insert_logic_block_(const uint64_t logic_block_id, const std::string& index_path);

      private:
        SupberBlockManager super_block_manager_;
        LogicBlockManager logic_block_manager_;
        PhysicalBlockManager physical_block_manager_;
    };
  }/** end namespace dataserver **/
}/** end namespace tfs **/

#endif /* TFS_DATASERVER_BLOCK_MANAGER_H_ */
