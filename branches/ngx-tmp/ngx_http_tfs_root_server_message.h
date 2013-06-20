
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#ifndef _NGX_HTTP_TFS_ROOT_SERVER_MESSAGE_H_INCLUDED_
#define _NGX_HTTP_TFS_ROOT_SERVER_MESSAGE_H_INCLUDED_


#include <ngx_http_tfs.h>


ngx_chain_t *ngx_http_tfs_root_server_create_message(ngx_pool_t *pool);
ngx_int_t ngx_http_tfs_root_server_parse_message(ngx_http_tfs_t *t);


#endif  /* _NGX_HTTP_TFS_ROOT_SERVER_MESSAGE_H_INCLUDED_ */
