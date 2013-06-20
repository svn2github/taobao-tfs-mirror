
/*
 * taobao tfs s3 for nginx
 *
 * This module is designed to support restful interface to tfs s3
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */

#ifndef _NGX_HTTP_AUTHORIZE_MODULE_H_INCLUDED_
#define _NGX_HTTP_AUTHORIZE_MODULE_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_http_hmac_sha1.h>


ngx_int_t
ngx_http_authorize_handler(ngx_http_tfs_t *t);
#endif
