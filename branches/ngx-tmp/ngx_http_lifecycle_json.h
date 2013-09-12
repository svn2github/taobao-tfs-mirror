
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_JSON_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_JSON_H_INCLUDED_


#include <yajl/yajl_parse.h>
#include <yajl/yajl_gen.h>
#include <yajl/yajl_tree.h>
#include <ngx_http_lifecycle_protocol.h>


typedef struct {
    yajl_gen          gen;
    ngx_log_t        *log;
    ngx_pool_t       *pool;
} ngx_http_lifecycle_json_gen_t;


ngx_http_lifecycle_json_gen_t *ngx_http_lifecycle_json_init(ngx_log_t *log, ngx_pool_t *pool);

void ngx_http_lifecycle_json_destroy(ngx_http_lifecycle_json_gen_t *tj_gen);


ngx_chain_t *ngx_http_lifecycle_json_lifecycle_info(ngx_http_lifecycle_json_gen_t *tj_gen,
    ngx_str_t *file_name, ngx_int_t expire_time);

ngx_chain_t *ngx_http_lifecycle_json_errorno(ngx_http_lifecycle_json_gen_t *tj_gen, int32_t error_no);
/*
ngx_chain_t * ngx_http_lifecycle_json_file_hole_info(ngx_http_lifecycle_json_gen_t *tj_gen, ngx_array_t *file_holes);
*/

#endif  /* _NGX_HTTP_LIFECYCLE_JSON_H_INCLUDED_ */
