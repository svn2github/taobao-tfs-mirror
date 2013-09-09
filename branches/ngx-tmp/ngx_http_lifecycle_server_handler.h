
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_SERVER_HANDLER_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_SERVER_HANDLER_H_INCLUDED_


#include <ngx_http_lifecycle.h>


/* root server */
ngx_int_t ngx_http_lifecycle_create_rs_request(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_rs(ngx_http_lifecycle_t *t);


/* meta server */
ngx_int_t ngx_http_lifecycle_create_ms_request(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ms_input_filter(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ms(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ms_get_bucket(ngx_http_lifecycle_t *t);


/* rc server */
ngx_int_t ngx_http_lifecycle_create_rcs_request(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_rcs(ngx_http_lifecycle_t *t);


/* name server */
ngx_int_t ngx_http_lifecycle_create_ns_request(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ns(ngx_http_lifecycle_t *t);


/* data server */
ngx_int_t ngx_http_lifecycle_create_ds_request(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ds_input_filter(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ds(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_process_ds_read(ngx_http_lifecycle_t *t);

ngx_int_t ngx_http_lifecycle_retry_ds(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_retry_ns(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_retry_ms(ngx_http_lifecycle_t *t);


#endif  /*  */

