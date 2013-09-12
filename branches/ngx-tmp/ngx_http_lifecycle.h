
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_H_INCLUDED_


#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_lifecycle_common.h>
#include <ngx_http_lifecycle_json.h>
#include <ngx_http_lifecycle_protocol.h>
#include <ngx_http_lifecycle_restful.h>
#include <ngx_http_lifecycle_connection_pool.h>
#include <ngx_http_lifecycle_peer_connection.h>

extern ngx_str_t   ngx_lifecycle_onoff;
extern ngx_uint_t  ngx_lifecycle_onoff_index;
extern ngx_str_t   ngx_lifecycle_filename;
extern ngx_uint_t  ngx_lifecycle_filename_index;
extern ngx_str_t   ngx_lifecycle_version;
extern ngx_uint_t  ngx_lifecycle_version_index;
extern ngx_str_t   ngx_lifecycle_expiretime;
extern ngx_uint_t  ngx_lifecycle_expiretime_index;
extern ngx_str_t   ngx_lifecycle_appkey;
extern ngx_uint_t  ngx_lifecycle_appkey_index;
extern ngx_str_t   ngx_lifecycle_action;
extern ngx_uint_t  ngx_lifecycle_action_index;
extern ngx_str_t   ngx_lifecycle_timetype;
extern ngx_uint_t  ngx_lifecycle_timetype_index;

typedef ngx_table_elt_t *(*ngx_http_lifecycle_create_header_pt)(ngx_http_request_t *r);


typedef struct {
    ngx_str_t            state_msg;
    ngx_int_t            state;
} ngx_http_lifecycle_state_t;


typedef struct {
    ngx_msec_t                       timeout;

    size_t                           max_temp_file_size;
    size_t                           temp_file_write_size;

    size_t                           busy_buffers_size_conf;


    ngx_flag_t                       ignore_client_abort;
    ngx_flag_t                       intercept_errors;

    uint64_t                         meta_root_server;
    ngx_http_lifecycle_kv_meta_table_t     meta_server_table;
    ngx_uint_t                       meta_access_count;
    ngx_uint_t                       meta_fail_count;
    ngx_uint_t                       update_kmt_interval_count;
    ngx_uint_t                       update_kmt_fail_count;
} ngx_http_lifecycle_loc_conf_t;


typedef struct {
    struct sockaddr_in               local_addr;
    u_char                           local_addr_text[NGX_INET_ADDRSTRLEN];

    ngx_log_t                       *log;
    ngx_log_t                       *deprecated_oper_log;
    ngx_log_t                       *forbidden_oper_log;
} ngx_http_lifecycle_srv_conf_t;


typedef struct {
    ngx_msec_t                               lifecycle_connect_timeout;
    ngx_msec_t                               lifecycle_send_timeout;
    ngx_msec_t                               lifecycle_read_timeout;

    size_t                                   send_lowat;
    size_t                                   buffer_size;
    size_t                                   body_buffer_size;
    size_t                                   busy_buffers_size;

    ngx_http_lifecycle_connection_pool_t              *conn_pool;

    ngx_addr_t                              *krs_addr;
} ngx_http_lifecycle_main_conf_t;


typedef ngx_int_t (*lifecycle_peer_handler_pt)(ngx_http_lifecycle_t *t);
typedef void (*ngx_http_lifecycle_handler_pt)(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
typedef ngx_int_t (*ngx_http_lifecycle_sub_process_pt)(ngx_http_lifecycle_t *t);


typedef struct {
    ngx_list_t                       headers;

    ngx_uint_t                       status_n;
    ngx_str_t                        status_line;

    ngx_table_elt_t                 *status;
    ngx_table_elt_t                 *date;
    ngx_table_elt_t                 *server;
    ngx_table_elt_t                 *connection;

    ngx_table_elt_t                 *expires;
    ngx_table_elt_t                 *etag;
    ngx_table_elt_t                 *x_accel_expires;
    ngx_table_elt_t                 *x_accel_redirect;
    ngx_table_elt_t                 *x_accel_limit_rate;

    ngx_table_elt_t                 *content_type;
    ngx_table_elt_t                 *content_length;

    ngx_table_elt_t                 *last_modified;
    ngx_table_elt_t                 *location;
    ngx_table_elt_t                 *accept_ranges;
    ngx_table_elt_t                 *www_authenticate;

#if (NGX_HTTP_GZIP)
    ngx_table_elt_t                 *content_encoding;
#endif

    off_t                            content_length_n;

    ngx_array_t                      cache_control;
} ngx_http_lifecycle_headers_in_t;


struct ngx_http_lifecycle_s {
    ngx_http_lifecycle_handler_pt           read_event_handler;
    ngx_http_lifecycle_handler_pt           write_event_handler;

    ngx_http_lifecycle_peer_connection_t   *lifecycle_peer;
    ngx_http_lifecycle_peer_connection_t   *lifecycle_peer_servers;
    uint8_t                                 lifecycle_peer_count;

    ngx_http_lifecycle_loc_conf_t          *loc_conf;
    ngx_http_lifecycle_srv_conf_t          *srv_conf;
    ngx_http_lifecycle_main_conf_t         *main_conf;

    ngx_http_lifecycle_restful_ctx_t        r_ctx;

    u_char                            *start_output;
    u_char                            *last_output;
    ngx_output_chain_ctx_t            output;
    ngx_chain_writer_ctx_t            writer;

    ngx_chain_t                      *request_bufs;
    ngx_chain_t                      *send_body;
    ngx_pool_t                       *pool;

    ngx_buf_t                         header_buffer;

    ngx_chain_t                      *recv_chain;

    ngx_chain_t                      *out_bufs;
    ngx_chain_t                      *busy_bufs;
    ngx_chain_t                      *free_bufs;

    ngx_http_lifecycle_json_gen_t    *json_output;

    /* header pointer */
    void                             *header;
    ngx_int_t                         header_size;
    size_t                            body_phase1_size;

    lifecycle_peer_handler_pt               create_request;
    lifecycle_peer_handler_pt               input_filter;
    lifecycle_peer_handler_pt               retry_handler;
    lifecycle_peer_handler_pt               process_request_body;
    lifecycle_peer_handler_pt               finalize_request;
    lifecycle_peer_handler_pt               decline_handler;

    void                             *finalize_data;
    void                             *data;

    ngx_int_t                         request_sent;
    ngx_uint_t                        sent_size;
    off_t                             length;

    ngx_log_t                        *log;

    ngx_int_t                         parse_state;
    ngx_int_t                         parse_body_phase;

    ngx_uint_t                        kv_meta_retry;

    /* final file name */
    ngx_int_t                         state;
    ngx_int_t                         orig_state;

    ngx_http_lifecycle_headers_in_t   headers_in;

    ngx_uint_t                        status;
    ngx_str_t                         status_line;
    /* kvmeta return rc */
    ngx_int_t                         lifecycle_status;

    uint64_t                          output_size;

    /* file info */
    int32_t                           file_version;



    /* for parallel write segments */
    uint32_t                          sp_count;
    uint32_t                          sp_done_count;
    uint32_t                          sp_fail_count;
    uint32_t                          sp_succ_count;
    uint32_t                          sp_curr;
    unsigned                          sp_ready:1;

    unsigned                          header_only:1;
    unsigned                          use_dedup:1;
    unsigned                          is_stat_dup_file:1;
    unsigned                          is_large_file:1;
    unsigned                          is_process_meta_seg:1;
    unsigned                          has_split_frag:1;
    unsigned                          retry_curr_ns:1;
    unsigned                          request_timeout:1;
    unsigned                          is_deprecated_oper:1;
    unsigned                          is_forbidden_oper:1;
    unsigned                          client_abort:1;
    unsigned                          is_rolling_back:1;
};


ngx_int_t ngx_http_lifecycle_init(ngx_http_lifecycle_t *t);
void ngx_http_lifecycle_finalize_request(ngx_http_request_t *r,
    ngx_http_lifecycle_t *t, ngx_int_t rc);
void ngx_http_lifecycle_finalize_state(ngx_http_lifecycle_t *t, ngx_int_t rc);
ngx_int_t ngx_http_lifecycle_reinit(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_connect(ngx_http_lifecycle_t *t);

ngx_int_t ngx_http_lifecycle_send_body(ngx_http_lifecycle_t *t);
ngx_int_t ngx_http_lifecycle_misc_ctx_init(ngx_http_lifecycle_t *t);
void ngx_http_lifecycle_access_recover(ngx_http_request_t *r);
void ngx_http_lifecycle_dummy_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
#endif /* _NGX_LIFECYCLE_H_INCLUDED_ */

