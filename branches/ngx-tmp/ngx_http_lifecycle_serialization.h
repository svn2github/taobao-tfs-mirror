
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_SERIALIZATION_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_SERIALIZATION_H_INCLUDED_


#include <ngx_lifecycle_common.h>

ngx_int_t ngx_http_lifecycle_serialize_string(u_char **p, ngx_str_t *string);

ngx_int_t ngx_http_lifecycle_deserialize_string(u_char **p, ngx_pool_t *pool,
    ngx_str_t *string);

//ngx_int_t ngx_http_lifecycle_serialize_vstring(u_char **p, ngx_str_t *string);

ngx_int_t ngx_http_lifecycle_deserialize_vstring(u_char **p, ngx_pool_t *pool,
    uint32_t *count, ngx_str_t **string);

ngx_int_t ngx_http_lifecycle_serialize_vuint64(u_char **p, uint32_t count,
    uint64_t *elts);

ngx_int_t ngx_http_lifecycle_deserialize_kv_meta_table(u_char **p,
    ngx_http_lifecycle_kv_meta_table_t *kv_meta_table);

#endif   /* _NGX_HTTP_LIFECYCLE_SERIALIZATION_H_INCLUDED_ */
