
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#ifndef _NGX_HTTP_TFS_KV_META_SERVER_MESSAGE_H_INCLUDED_
#define _NGX_HTTP_TFS_KV_META_SERVER_MESSAGE_H_INCLUDED_


#include <ngx_http_tfs.h>


ngx_chain_t *ngx_http_tfs_meta_server_create_message(ngx_http_tfs_t *t);
ngx_int_t ngx_http_tfs_meta_server_parse_message(ngx_http_tfs_t *t);
ngx_http_tfs_inet_t *ngx_http_tfs_select_meta_server(ngx_http_tfs_t *t);
ngx_chain_t *ngx_http_tfs_meta_auth_create_message(ngx_http_tfs_t *t);
ngx_int_t ngx_http_tfs_meta_auth_parse_message(ngx_http_tfs_t *t);


#endif  /* _NGX_HTTP_TFS_KV_META_SERVER_MESSAGE_H_INCLUDED_ */
