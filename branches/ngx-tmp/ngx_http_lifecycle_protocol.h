
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_PROTOCOL_H_
#define _NGX_HTTP_LIFECYCLE_PROTOCOL_H_

#include "ngx_lifecycle_common.h"

#define NGX_HTTP_LIFECYCLE_PACKET_FLAG            0x4E534654      /* LIFECYCLEN */
#define NGX_HTTP_LIFECYCLE_PACKET_VERSION         2

#define NGX_HTTP_LIFECYCLE_RAW_FILE_INFO_SIZE     sizeof(ngx_http_lifecycle_raw_file_info_t)

#define NGX_HTTP_LIFECYCLE_UNKNOWN_STATUS         -1


typedef enum
{
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE = 1,
    NGX_HTTP_LIFECYCLE_REQ_KV_RT_GET_TABLE_MESSAGE = 352,
    NGX_HTTP_LIFECYCLE_RSP_KV_RT_GET_TABLE_MESSAGE = 353,
    NGX_HTTP_LIFECYCLE_REQ_KV_SET_LIFE_CYCLE_MESSAGE = 382,
    NGX_HTTP_LIFECYCLE_REQ_KV_GET_LIFE_CYCLE_MESSAGE = 383,
    NGX_HTTP_LIFECYCLE_RSP_KV_GET_LIFE_CYCLE_MESSAGE = 384,
    NGX_HTTP_LIFECYCLE_REQ_KV_DEL_LIFE_CYCLE_MESSAGE = 385,
    NGX_HTTP_LIFECYCLE_LOCAL_PACKET = 500
} ngx_http_lifecycle_status_msg_e;


typedef enum
{
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_OK = 0,
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_ERROR,
    NGX_HTTP_LIFECYCLE_STATUS_NEED_SEND_BLOCK_INFO,
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_PING,
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_REMOVE,
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_BLOCK_FULL,
    NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_ACCESS_DENIED
} ngx_http_lifecycle_message_status_t;


typedef enum
{
    NGX_HTTP_LIFECYCLE_OPEN_MODE_DEFAULT = 0,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_READ = 1,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_WRITE = 2,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_CREATE = 4,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_NEWBLK = 8,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_NOLEASE = 16,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_STAT = 32,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_LARGE = 64,
    NGX_HTTP_LIFECYCLE_OPEN_MODE_UNLINK = 128
} ngx_http_lifecycle_open_mode_e;


typedef enum
{
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_EXPBLK = 1,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_LOADBLK,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_COMPACT,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_IMMEDIATELY_REPL,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_REPAIR_GROUP,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_SET_PARAM,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_UNLOADBLK,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_FORCE_DATASERVER_REPORT,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_ROTATE_LOG,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_GET_BALANCE_PERCENT,
    NGX_HTTP_LIFECYCLE_CLIENT_CMD_SET_BALANCE_PERCENT
} ngx_http_lifecycle_ns_ctl_type_e;


typedef enum
{
    NGX_HTTP_LIFECYCLE_ACCESS_FORBIDEN = 0,
    NGX_HTTP_LIFECYCLE_ACCESS_READ_ONLY = 1,
    NGX_HTTP_LIFECYCLE_ACCESS_READ_AND_WRITE = 2,
} ngx_http_lifecycle_access_type_e;


typedef struct {
    uint32_t                                flag;
    uint32_t                                len;
    uint16_t                                type;
    uint16_t                                version;
    uint64_t                                id;
    uint32_t                                crc;
} NGX_PACKED ngx_http_lifecycle_header_t;


typedef struct {
    int32_t                                 code;
    uint32_t                                error_len;
    u_char                                  error_str[];
} NGX_PACKED ngx_http_lifecycle_status_msg_t;


/* root server */
typedef struct {
    ngx_http_lifecycle_header_t                   header;
    uint8_t                                 reserse;
} NGX_PACKED ngx_http_lifecycle_rs_request_t;


typedef struct {
    /* ignore header */
    uint64_t                                version;
    uint64_t                                length;
    u_char                                  table[];
} NGX_PACKED ngx_http_lifecycle_rs_response_t;


/* kv root server */
typedef struct {
    ngx_http_lifecycle_header_t                   header;
    uint8_t                                 reserse;
} NGX_PACKED ngx_http_lifecycle_kv_rs_request_t;


/* meta server */
typedef struct {
    uint32_t        block_id;
    uint64_t        file_id;
    int64_t         offset;
    uint32_t        size;
} NGX_PACKED ngx_http_lifecycle_meta_frag_meta_info_t;


typedef struct {
    uint32_t                               cluster_id;

    /* highest is split flag */
    uint32_t                               frag_count;
    ngx_http_lifecycle_meta_frag_meta_info_t     frag_meta[];
} NGX_PACKED ngx_http_lifecycle_meta_frag_info_t;


typedef struct {
    ngx_http_lifecycle_header_t                   header;
    uint64_t                                app_id;
    uint64_t                                user_id;
    uint32_t                                file_len;
    u_char                                  file_path_s[];
} NGX_PACKED ngx_http_lifecycle_ms_base_msg_header_t;


typedef struct {
    ngx_http_lifecycle_header_t                   header;
    uint64_t                                app_id;
    uint64_t                                user_id;
    int64_t                                 pid;
    uint32_t                                file_len;
    u_char                                  file_path[];
} NGX_PACKED ngx_http_lifecycle_ms_ls_msg_header_t;


typedef struct {
    /* ignore header */
    uint8_t               still_have;
    uint32_t              count;
} NGX_PACKED ngx_http_lifecycle_ms_ls_response_t;


typedef struct {
    /* ignore header */
    uint8_t                           still_have;
    ngx_http_lifecycle_meta_frag_info_t     frag_info;
} NGX_PACKED ngx_http_lifecycle_ms_read_response_t;


/* kv meta server  */
typedef struct {
    ngx_http_lifecycle_header_t               header;
    int32_t                                   file_type;
    uint32_t                                  file_name_len;
    u_char                                    file_name[];
} NGX_PACKED ngx_http_kv_ms_set_lifecycle_request_t;


typedef struct {
    ngx_http_lifecycle_header_t               header;
    int32_t                                   file_type;
    uint32_t                                  file_name_len;
    u_char                                    file_name[];
} NGX_PACKED ngx_http_kv_ms_get_lifecycle_request_t;


typedef struct {
    ngx_http_lifecycle_header_t               header;
    int32_t                                   file_type;
    uint32_t                                  file_name_len;
    u_char                                    file_name[];
} NGX_PACKED ngx_http_kv_ms_del_lifecycle_request_t;


typedef struct {
    /* ignore header */
    uint32_t                                  invalid_time_s;
} NGX_PACKED ngx_http_kv_ms_get_lifecycle_response_t;



/* rc server  */
typedef struct {
    ngx_http_lifecycle_header_t                   header;
    uint32_t                                appkey_len;
    u_char                                  appkey[];
 /* uint64_t                                app_ip */
} NGX_PACKED ngx_http_lifecycle_rcs_login_msg_header_t;


#endif  /* _NGX_HTTP_LIFECYCLE_PROTOCOL_H_ */
