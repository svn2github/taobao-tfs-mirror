
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_KV_ROOT_SERVER_MESSAGE_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_KV_ROOT_SERVER_MESSAGE_H_INCLUDED_


#include <ngx_http_lifecycle.h>


ngx_chain_t *ngx_http_lifecycle_root_server_create_message(ngx_pool_t *pool);
ngx_int_t ngx_http_lifecycle_root_server_parse_message(ngx_http_lifecycle_t *t);


#endif  /* _NGX_HTTP_LIFECYCLE_KV_ROOT_SERVER_MESSAGE_H_INCLUDED_ */
