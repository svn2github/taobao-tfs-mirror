
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_RESTFUL_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_RESTFUL_H_INCLUDED_


#include <ngx_core.h>
#include <ngx_http.h>

typedef struct {

    /*---------------------*/
    ngx_int_t                    lifecycle_on;
    ngx_int_t                    absolute_time;
    ngx_str_t                    file_name;
    uint8_t                      version;
    ngx_str_t                    appkey;
    ngx_http_lifecycle_action_t  action;
    uint8_t                      expiretime_type;

    uint64_t                     app_id;
    uint64_t                     user_id;

    uint8_t                      file_type;
    ngx_str_t                    file_path_s;
    ngx_str_t                    file_path_d;

} ngx_http_lifecycle_restful_ctx_t;


ngx_int_t ngx_http_restful_parse(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx);


#endif  /* _NGX_HTTP_LIFECYCLE_RESTFUL_H_INCLUDED_ */
