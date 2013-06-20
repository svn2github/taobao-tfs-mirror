
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_HMAC_SHA1_H_INCLUDED_
#define _NGX_HTTP_HMAC_SHA1_H_INCLUDED_

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

int hmac_sha1(const u_char* secret, ngx_uint_t secret_len,
    const u_char* str_to_sign, ngx_uint_t str_to_sign_len,
    u_char* signature, ngx_uint_t max_signature_size, ngx_uint_t *signature_len);

#endif
