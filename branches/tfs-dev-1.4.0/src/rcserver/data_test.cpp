/*
* (C) 2007-2010 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
*
* Version: $Id
*
* Authors:
*   daoan <daoan@taobao.com>
*      - initial release
*
*/
#include "mysql_database_helper.h"
using namespace tfs;
using namespace tfs::rcserver;
int main()
{
  MysqlDatabaseHelper hl;
  hl.set_conn_param("10.232.31.33:3306:tfs_stat", "tfs", "tfs_stat#2012");
  int ret;
  ret = hl.connect();
  printf("ret = %d\n", ret);
  {
    BaseInfoUpdateTime outparam;
    ret = hl.select(outparam);
    printf("\nBaseInfoUpdateTime ret = %d\n", ret);
    printf("base_last_update_time_ %s app_last_update_time_ %s\n",
        tbutil::Time(outparam.base_last_update_time_).toDateTime().c_str(), 
          tbutil::Time(outparam.app_last_update_time_).toDateTime().c_str());
  }
  {
    MIdAppInfo outparam;
    ret = hl.scan(outparam);
    printf("\nMIdAppInfo ret = %d\n", ret);
    MIdAppInfo::iterator it;
    for (it = outparam.begin(); it != outparam.end(); it++ )
    {
      printf("key_ %s id %d quto %ld cluster_group_id %d app_name %s app_owner %s"
          " report_interval %d need_duplicate %d rem %s modifytime %s\n", 
          it->second.key_, it->second.id_, it->second.quto_, it->second.cluster_group_id_, it->second.app_name_,
          it->second.app_owner_, it->second.report_interval_, it->second.need_duplicate_,
          it->second.rem_, tbutil::Time(it->second.modify_time_).toDateTime().c_str()
          );
    }
    hl.close();
  }
  {
    VResourceServerInfo outparam;
    ret = hl.scan(outparam);
    printf("\nVResourceServerInfo ret = %d\n", ret);
    for (int i = 0; i < outparam.size(); i++)
    {
      printf("%s %d %s\n", outparam[i].addr_info_, outparam[i].stat_, outparam[i].rem_);
    }
    hl.close();
  }
  {
    VClusterRackDuplicateServer outparam;
    ret = hl.scan(outparam);
    printf("\nVClusterRackDuplicateServer ret = %d\n", ret);
    VClusterRackDuplicateServer::iterator it;
    for (it = outparam.begin(); it != outparam.end(); it++ )
    {
      printf("cluster_rack_id_ %d dupliate_server_addr_ %s \n",
          it->cluster_rack_id_, it->dupliate_server_addr_);
    }
  }
  {
    VClusterRackGroup outparam;
    ret = hl.scan(outparam);
    printf("\nVClusterRackGroup ret = %d\n", ret);
    VClusterRackGroup::iterator it;
    for (it = outparam.begin(); it != outparam.end(); it++ )
    {
      printf("cluster_group_id_ %d cluster_rack_id_ %d cluster_rack_access_type_ %d rem %s \n",
          it->cluster_group_id_, it->cluster_rack_id_, 
          it->cluster_rack_access_type_, it->rem_);
    }
  }
  {
    VClusterRackInfo outparam;
    ret = hl.scan(outparam);
    printf("\nVClusterRackInfo ret = %d\n", ret);
    for (int i = 0; i < outparam.size(); i++)
    {
      printf("cluster_rack_id_ %d rem_ %s cluster_stat_ %d access_type_ %d cluster_id_ %s ns_vip_ %s\n", outparam[i].cluster_rack_id_, outparam[i].rem_, 
          outparam[i].cluster_data_.cluster_stat_,
          outparam[i].cluster_data_.access_type_,
          outparam[i].cluster_data_.cluster_id_.c_str(),
          outparam[i].cluster_data_.ns_vip_.c_str());
    }
  }
  hl.update_session_info();
  hl.update_session_stat();
  hl.update_app_stat();
  return 0;
}
