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

#ifndef TFS_DATASERVER_CLIENT_REQUEST_SERVER_H_
#define TFS_DATASERVER_CLIENT_REQUEST_SERVER_H_

class ClientRequestServer
{
public:
	ClientRequestServer(const uint64_t ns_ip_port, const uint64_t ds_ip_port_);
	virtual ~ClientRequestServer();
	int create_file_id(uint64_t& file_id, uint64_t& file_number,const uint64_t block_id);
	int write(common::BlockInfoV2& block_info, WriteLease* lease,const int32_t remote_version,
			const common::WriteDataInfo& write_info, const char* buf);
	int write(const uint64_t block_id, const int32_t offset, const int32_t length, const char* buf);
	int close(int32_t& write_file_size, const CloseFileInfo& info);
	int read(int32_t& real_len, char* buf, const uint64_t block_id, const uint64_t fileid,
			const int32_t offset, const int8_t flag) const;
	int read_degrade(int32_t& real_len, char* buf, const const uint64_t block_id, const uint64_t fileid,
			const int32_t offset, const int8_t flag, const common::FamilyInfoExt& family_info) const;
	int stat(common::FileInfoV2& info, const uint64_t block_id, const uint64_t fileid, const int32_t mode) const;
	int unlink(common::BlockInfoV2& info, int32_t& file_size, const uint64_t block_id, const uint64_t fileid,
			const int32_t action, const int32_t remote_version);

	int batch_new_block(const std::vector<uint64_t>& blocks);
	int batch_del_block(const std::vector<uint64_t>& blocks);

	int get_all_logic_block_to_physical_block(std::map<uint64, std::vector<uint32_t> >& blocks) const;
	int get_all_block_id(std::vector<BlockInfoV2>& blocks) const;

	int write_file_infos(std::vector<FileInfoV2>& infos,  const uint64_t block_id = common::INVALID_BLOCK_ID);
	int read_file_infos(std::vector<FileInfoV2>& infos,const uint64_t block_id = common::INVALID_BLOCK_ID) const;

	int commit_write_complete_to_ns(const uint64_t block_id, const uint64_t lease_id, const int32_t status,
			const common::UnlinkFlag unlink_flag = common::UNLINK_FLAG_NO);
	int request_update_block_info(const uint64_t block_id, const common::UpdateBlockType repair = common::UPDATE_BLOCK_NORMAL);
private:
	uint64_t ns_ip_port_;
	uint64_t ds_ip_port_;
};
#endif /* CLIENT_REQUEST_SERVER_H_ */
