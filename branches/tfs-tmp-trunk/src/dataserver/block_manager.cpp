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

namespace tfs
{
  namespace dataserver
  {
    BlockManager::BlockManager()
    {

    }

    BlockManager::~BlockManager()
    {

    }

    int BlockManager::format(const common::FileSystemParameter& parameter)
    {
      //1.initialize supber block
      int32_t ret = create_file_system_superblock_(parameter);

      //2.create mount directory
      if (TFS_SUCCESS == ret)
      {
        ret = create_file_system_dir_(parameter);
      }

      //3.pre-allocate create main block
      if (TFS_SUCCESS == ret)
      {
        ret = fallocate_block_(parameter);
      }
      return ret;
    }

    int BlockManager::cleanup(FileSystemParameter& parameter)
    {
      return DirectoryOp::delete_directory_recursively(parameter.mount_name_) ? TFS_SUCCESS : EXIT_RM_DIR_ERROR;
    }

    int BlockManager::bootstrap(const common::FileSystemParameter& parameter)
    {
      //1. load supber block
      int32_t ret = load_supber_(parameter);

      //2. cleanup dirty index files
      if (TFS_SUCCESS == ret)
      {
        ret = cleanup_dirty_index_(parameter);
      }

      //3. load all block
      if (TFS_SUCCESS == ret)
      {
        ret = load_index_(parameter);
      }
      return ret;
    }

    int BlockManager::new_block(const uint64_t logic_block_id)
    {

    }

    int BlockManager::new_ext_block(const uint64_t logic_block_id)
    {

    }

    int BlockManager::del_block(const uint64_t logic_block_id)
    {

    }

    LogicBlock* BlockManager::get(const uint64_t logic_block_id) const
    {

    }

    int BlockManager::get_all_block_info(std::set<BlockInfo>& blocks) const
    {

    }

    int BlockManager::get_all_block_info(std::set<common::BlockInfoExt>& blocks) const
    {

    }

    int BlockManager::get_all_logic_block_to_physical_block(std::map<uint64, std::vector<uint32_t> >& blocks) const
    {

    }

    int BlockManager::get_all_block_id(std::vector<BlockInfoV2>& blocks) const
    {

    }

    int BlockManager::get_all_logic_block_count() const
    {

    }

    int BlockManager::get_space(int64_t& total_space, int64_t& used_space) const
    {

    }

    int BlockManager::get_superblock_info(common::SuperBlockInfo& info) const
    {

    }

    int BlockManager::switch_block_from_tmp(const uint64_t logic_block_id)
    {

    }

    int BlockManager::timeout(const time_t now)
    {

    }

    int BlockManager::load_supber_block_(const common::FileSystemParameter& parameter)
    {
      SupberBlockInfo info;
      int32_t ret =  get_super_block_manager().load();
      if (TFS_SUCCESS != ret)
        TBSYS_LOG(ERROR, "load super block error, ret: %d, errors: %d, %s", ret, errno, strerror(errno));
      if (TFS_SUCCESS == ret)
      {
        ret = get_super_block_manager().get_super_block_info(info);
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "read supber block information error, ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == memcmp(DEV_TAG, super_block.mount_tag_, strlen(DEV_TAG) + 1) ? TFS_SUCCESS : EXIT_FS_NOTINIT_ERROR;
        if (TFS_SUCCESS != ret)
          TBSYS_LOG(ERROR, "file system not initialized. please format it first, ret: %d", ret);
      }
      if (TFS_SUCCESS == ret)
      {
        ret = 0 == parameter.mount_name_.compare(info.mount_point_) ? TFS_SUCCESS : EXIT_MOUNT_POINT_ERROR;
        if (TFS_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "file system mount point conflict, please check mount point!, point: %s != %s, ret: %d",
              parameter.mount_name_.c_str(), info.mount_point_, ret);
        }
      }
      return ret;
    }

    int BlockManager::load_index_(const common::FileSystemParameter& parameter)
    {
      SupberBlockInfo info;
      int32_t ret = get_supber_block_manager().get_supber_block_info(info);
      if (TFS_SUCCESS == ret)
      {
        BlockIndex index;
        for (int32_t id = 0; id < info.used_main_block_count_ && TFS_SUCCESS == ret; ++id)
        {
          ret = get_supber_block_manager().get_block_index(index, id);
          if (TFS_SUCCESS == ret)
          {
            if (INVALID_BLOCK_ID == index.logic_block_id_)
            {
              memset(&index, 0, sizeof(index));
              ret = get_supber_block_manager().update_block_index(index);
              if (TFS_SUCCESS == ret)
                ret = get_supber_block_manager().flush();
              continue;
            }

            bool complete = (BLOCK_CREATE_COMPLETE_STATUS_COMPLETE == index.status_);

            PhysicalBlock* physical_block = NULL;
            std::stringstream index_path;
            index_path << info.mount_point_ << INDEX_DIR_PREFIX << index.logic_block_id_ << "." << index.physical_block_id_;
            LogicBlock* logic_block = insert_logic_block_(index.logic_block_id_, index_path.str());
            ret = (NULL != logic_block) ? TFS_SUCCESS : EXIT_ADD_LOGIC_BLOCK_ERROR;
            if (TFS_SUCCESS == ret)
            {
              std::stringstream physical_block_path;
              physical_block_path << info.mount_point_ << MAINBLOCK_DIR_PREFIX << index.physical_file_name_id_;
              physical_block = insert_physical_block_(index, index.physical_block_id_, physical_block_path.str());
              ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
            }

            if (TFS_SUCCESS == ret)
            {
              if (BLOCK_SPLIT_STATUS_NO == index.split_)
                ret = logic_block.add_physic_block(physical_block);
            }

            if (TFS_SUCCESS == ret)
            {
              BlockIndex ext_index = index;
              while (INVALID_BLOCK_ID != ext_index.next_index_ && TFS_SUCCESS == ret)
              {
                ret = get_supber_block_manager().get_block_index(ext_index, ext_index.next_index_);
                if (TFS_SUCCESS == ret)
                {
                  if (!complete)
                    complete = (BLOCK_CREATE_COMPLETE_STATUS_COMPLETE == ext_index.status_);
                  if (INVALID_BLOCK_ID == ext_index.logic_block_id_
                      || index.logic_block_id_ != ext_index.logic_block_id_
                      || id != ext_index.prev_index_)
                  {
                    memset(&ext_index, 0, sizeof(ext_index));
                    ret = get_supber_block_manager().update_block_index(ext_index);
                    if (TFS_SUCCESS == ret)
                      ret = get_supber_block_manager().flush();
                    continue;
                  }

                  std::stringstream ext_physical_block_path;
                  ext_physical_block_path << info.mount_point_ << MAINBLOCK_DIR_PREFIX << ext_index.physical_file_name_id_;
                  physical_block = insert_physical_block_(ext_index, ext_index.physical_block_id_, ext_physical_block_path.str());
                  ret = (NULL != physical_block) ? TFS_SUCCESS : EXIT_ADD_PHYSICAL_BLOCK_ERROR;
                  if (TFS_SUCCESS == ret)
                  {
                    if (BLOCK_SPLIT_STATUS_NO == index.split_)
                      ret = logic_block.add_physic_block(physical_block);
                  }
                }
              }//end while
            }//end if (TFS_SUCCESS == ret)

            if (TFS_SUCCESS == ret)
            {
              if (!complete)
              {
                ret = del_block(index.logic_block_id_);
              }
              if (complete && TFS_SUCCESS == ret)
              {
                ret = logic_block->load_index(info.mmap_option_);
              }
              if (complete && TFS_SUCCESS == ret)
              {
                //检测LOGICBLOCK是否完整
                ret = logic_block->check_block_intact();
                if (TFS_SUCCESS != ret)
                {
                  //TODO,不完整的BLOCK是否需要直接删除？？？
                  ret = del_block(index.logic_block_id_);
                }
              }
            }
          }
        }//end for
      }//end if
      return ret;
    }

    static int index_filter(const struct dirent* entry)
    {
      return index(entry->d_name, '.') ? 1 : 0;
    }

    int BlockManager::cleanup_dirty_index_(const common::FileSystemParameter& parameter)
    {
      int32_t index = 0;
      struct dirent** names= NULL;
      std::string path(parameter.mout_point_ + INDEX_DIR_PREFIX);
      int32_t num = scandir(pat.c_str(), &names, index_filter, NULL);
      for (index = 0; index < num; ++index)
      {
        std::string file_path = path + names[index]->d_name;
        unlink(file_path.c_str());
        free(names[index]);
      }
      free(names);
      return TFS_SUCCESS;
    }

#define CHECK_VALUE_RANGE(current, max, min) (current >= min && current <= max)

    int BlockManager::create_file_system_superblock_(const FileSystemParameter& parameter)
    {
      SuperBlockInfo info;
      memset(&info, 0, sizeof(info));
      info.version_ = parameter.file_system_version_;
      memcpy(info.mount_tag_, DEV_TAG, sizeof(info.mount_tag_));
      strncpy(super_block_.mount_point_, parameter.mount_name_.c_str(), MAX_DEV_NAME_LEN - 1);
      info.mount_time_ = time(NULL);
      info.mount_point_use_space_ = parameter.max_mount_size_ * 1024;
      info.mount_fs_type_ = parameter.base_fs_type_;
      info.max_main_block_size_   = parameter.main_block_size_;
      info.max_extend_block_size_ = parameter.extend_block_size_;

      const int32_t MAX_INITIALIZE_INDEX_SIZE = 512;
      ret = (EXT4 != info.mount_fs_type_ && EXT3_FULL != info.mount_fs_type_ && EXT3_FTRUN != info.mount_fs_type_)
        ? EXIT_FS_TYPE_ERROR : TFS_SUCCESS;
      if (TFS_SUCCESS == ret)
      {
        ret = (CHECK_VALUE_RANGE(info.max_main_block_size_, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE)
            && CHECK_VALUE_RANGE(info.max_extend_block_size_, MAX_EXT_BLOCK_SIZE, MIN_EXT_BLOCK_SIZE)
            && info.max_ext_block_size_ <= info.max_extend_block_size_
            && info.max_main_block_size_ > 0 && info.max_ext_block_size_ > 0
            && 0 == info.max_main_block_size_ % 2  && 0 == info.max_ext_block_size_ % 2) ? TFS_SUCCESS : EXIT_BLOCK_SIZE_INVALID;

        if (TFS_SUCCESS == ret)
        {
          ret = parameter.avg_segment_size_ > 0 ? TFS_SUCCESS : EXIT_ARG_SEGMENT_SIZE_INVALID;
        }

        if (TFS_SUCCESS == ret)
        {
          const int32_t pagesize = getpagesize();
          const int32_t avg_file_count = info.max_main_block_size_ / parameter.avg_segment_size_;
          info.used_main_block_count_  = 0;
          info.used_extend_block_count_ = 0;
          info.total_main_block_count_ = info.mount_point_use_space_ / info.max_main_block_size_;
          info.hash_bucket_count_      = std::min(MAX_INITIALIZE_INDEX_SIZE, avg_file_count);
          const int32_t mmap_size = sizeof(IndexHeaderV2) + (info.hash_bucket_count_ + 1) * sizeof(FileInfoV2);
          const int32_t count     = mmap_size / pagesize;
          const int32_t remainder = mmap_size % pagesize;
          const int32_t max_mmap_size = sizeof(IndexHeaderV2) + (avg_file_count + 1) * sizeof(FileInfoV2);
          const int32_t max_count     = max_mmap_size / pagesize;
          const int32_t max_remainder = max_mmap_size % pagesize;
          info.mmap_option_.first_mmap_size_=  remainder ? (count + 1) * pagesize : count * pagesize;
          info.mmap_option_.per_mmap_size_  =  pagesize;
          if (max_remainder)
            info.mmap_option_.max_mmap_size_ = (count + 1) * pagesize * INNERFILE_MAX_MULTIPE;
          else
            info.mmap_option_.max_mmap_size_ = count * pagesize * INNERFILE_MAX_MULTIPE;
        }

        if (TFS_SUCCESS == ret)
        {
          ret = get_supber_block_manager().format(info);
        }
        return ret;
      }

      int BlockManager::create_file_system_dir_(const FileSystemParameter& parameter)
      {
        SuperBlockInfo info;
        int32_t ret = get_super_block_manager().get_super_block_info(info);
        //main block directory
        if (TFS_SUCCESS == ret)
        {
          ret = create_full_path(info.mount_point_, false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "mkdir main block directory error, errno: %d,%s", errno, strerror(errno));
        }

        //index directroy
        if (TFS_SUCCESS == ret)
        {
          std::string index_dir = super_block_.mount_point_
            index_dir += INDEX_DIR_PREFIX;
          ret = create_full_path(index_dir.c_str(), false, DIR_MODE) ? TFS_SUCCESS : EXIT_CREATE_DIR_ERROR;
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "mkdir index block directory error, errno: %d,%s", errno, strerror(errno));
        }
        return ret;
      }

      int BlockManager::fallocate_block_(const FileSystemParameter& parameter)
      {
        UNUSED(parameter);
        SuperBlockInfo info;
        FileFormater* file_formater = NULL;
        int32_t ret = get_super_block_manager().get_super_block_info(info);
        if (TFS_SUCCESS == ret)
        {
          if (EXT4 == info.mount_fs_type_)
            file_formater = new Ext4FileFormater();
          else if (EXT3_FULL == info.mount_fs_type_)
            file_formater = new Ext3FullFileFormater();
          else if (EXT3_FTRUN == info.mount_fs_type_)
            file_formater = new Ext3SimpleFileFormater();
          ret = NULL == file_formater ? EXIT_FS_TYPE_ERROR : TFS_SUCCESS;
          if (TFS_SUCCESS != ret)
            TBSYS_LOG(ERROR, "base filesystem type: %d not supported", info.mount_fs_type_);
        }

        if (TFS_SUCCESS == ret)
        {
          for (int32_t index = 0; index < info.total_main_block_count_ && TFS_SUCCESS == ret; ++index)
          {
            std::stringstream path;
            path << info.mount_point_ << MAINBLOCK_DIR_PREFIX << index;
            FileOperation file_op(path.str(), O_RDWR | O_CREAT);
            ret = file_op.open_file() < 0 ? EXIT_OPEN_FILE_ERROR : TFS_SUCCESS;
            if (TFS_SUCCESS != ret)
              TBSYS_LOG(ERROR, "allocate space error, open file failed, errno: %d, %s", errno, strerror(errno));
            if (TFS_SUCCESS == ret)
            {
              ret = file_formater->block_file_format(file_op.get_fd(), info.max_block_size_);
              if (TFS_SUCCESS != ret)
                TBSYS_LOG(ERROR, "allocate space error, allocate failed, errno: %d, %s", errno, strerror(errno));
            }
          }
        }
        tbsys::gDelete(file_formater);
        return ret;
      }

      int BlockManager::get_avail_physical_block_id_(uint32_t& physical_block_id)
      {

      }

      int BlockManager::del_logic_block_from_table_(const uint64_t logic_block_id)
      {

      }

      int BlockManager::del_logic_block_from_tmp_(const uint64_t logic_block_id)
      {

      }

      int BlockManager::rollback_superblock_(const uint32_t physical_block_id, const int8_t type, const bool modify)
      {

      }

      PhysicalBlock* BlockManager::insert_physical_block_(const SupberBlockInfo& info, const BlockIndex& index, const int32_t physical_block_id, const std::string& path)
      {
        PhysicalBlock* physical_block = NULL;
        int32_t ret = (INVALID_BLOCK_ID == physical_block_id || path.empty()) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret && !get_physical_block_manager().exist(physical_block_id))
        {
          //0 == index.index_完整的大数据块(主块，分割的块)
          const int32_t start = (0 == index.index_) ? BLOCK_RESERVER_LENGTH : (index.index_ - 1) * info.max_extend_block_size_;
          const int32_t end   = (0 == index.index_) ? (start + info.max_main_block_size_) : (start + info.max_extend_block_size_);
          ret = get_physical_block_manager().insert(index, physical_block_id, path, start, end);
        }
        if (TFS_SUCCESS == ret)
        {
          physical_block = get_physical_block_manager().get(physical_block_id);
        }
        return physical_block;
      }

      LogicBlock* BlockManager::insert_logic_block_(const uint64_t logic_block_id, const std::string& index_path)
      {
        LogicBlock* logic_block = NULL;
        int32_t ret = (INVALID_BLOCK_ID == logic_block_id || index_path.empty()) ? EXIT_PARAMETER_ERROR : TFS_SUCCESS;
        if (TFS_SUCCESS == ret && !get_logic_block_manager().exist(logic_block_id))
        {
          ret = get_logic_block_manager().insert(logic_block_id, index_path);
        }
        if (TFS_SUCCESS == ret)
        {
          logic_block = get_logic_block_manager().get(logic_block_id);
        }
        return logic_block;
      }
    }/** end namespace dataserver **/
  }/** end namespace tfs **/
