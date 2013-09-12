
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_KV_META_SERVER_MESSAGE_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_KV_META_SERVER_MESSAGE_H_INCLUDED_


#include <ngx_http_lifecycle.h>


ngx_chain_t *ngx_http_lifecycle_meta_server_create_message(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_meta_server_parse_message(ngx_http_lifecycle_t *t);
ngx_http_lifecycle_inet_t *ngx_http_lifecycle_select_meta_server(ngx_http_lifecycle_t *t);


#endif  /* _NGX_HTTP_LIFECYCLE_KV_META_SERVER_MESSAGE_H_INCLUDED_ */
