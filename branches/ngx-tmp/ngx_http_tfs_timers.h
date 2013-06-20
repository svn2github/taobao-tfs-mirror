
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#ifndef _NGX_HTTP_TFS_TIMERS_H_INCLUDED_
#define _NGX_HTTP_TFS_TIMERS_H_INCLUDED_


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_http_tfs.h>


ngx_int_t ngx_http_tfs_add_rcs_timers(ngx_cycle_t *cycle, ngx_http_tfs_main_conf_t *tmcf);
ngx_int_t ngx_http_tfs_timers_init(ngx_cycle_t *cycle, u_char *lock_file);


#endif  /* _NGX_HTTP_TFS_TIMERS_H_INCLUDED_ */
