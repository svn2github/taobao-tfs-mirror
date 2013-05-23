
/*
 * taobao tfs s3 for nginx
 *
 * This module is designed to support restful interface to tfs s3
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_S3_PARSE_H_INCLUDED_
#define _NGX_HTTP_S3_PARSE_H_INCLUDED_


#include <ngx_core.h>
#include <ngx_http.h>



typedef struct {
    ngx_str_t   bucket_name;    /*parse*/
    ngx_str_t   object_name;    /*parse*/
    ngx_str_t   access_id;      /*find*/
    ngx_str_t   signature_user; /*find*/

    ngx_str_t   method;         /*get*/
    ngx_str_t   content_md5;    /*find*/
    ngx_str_t   content_type;   /*get*/
    ngx_str_t   date;           /*find*/

} ngx_http_s3_parse_ctx_t;


ngx_int_t ngx_http_s3_parse(ngx_http_request_t *r, ngx_http_s3_parse_ctx_t *ctx);

#endif  /* _NGX_HTTP_S3_PARSE_H_INCLUDED_ */
