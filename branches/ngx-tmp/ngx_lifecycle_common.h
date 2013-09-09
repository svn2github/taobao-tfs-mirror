
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#ifndef _NGX_HTTP_LIFECYCLE_COMMON_H_INCLUDED_
#define _NGX_HTTP_LIFECYCLE_COMMON_H_INCLUDED_


#include <ngx_core.h>
#include <ngx_http.h>

#define NGX_PACKED __attribute__ ((__packed__))

#define NGX_HTTP_LIFECYCLE_HEADER                           0
#define NGX_HTTP_LIFECYCLE_BODY                             1
#define NGX_HTTP_LIFECYCLE_BODY_PHASE1                      2
#define NGX_HTTP_LIFECYCLE_BODY_PHASE2                      3

#define NGX_HTTP_LIFECYCLE_YES                              1
#define NGX_HTTP_LIFECYCLE_NO                               0
#define NGX_HTTP_LIFECYCLE_AGAIN                            -20

#define NGX_HTTP_LIFECYCLE_MD5_RESULT_LEN                   16

#define NGX_HTTP_LIFECYCLE_SERVER_COUNT                     2            /*  rs, ms */
#define NGX_HTTP_LIFECYCLE_METASERVER_COUNT                 1024
#define NGX_HTTP_LIFECYCLE_KV_META_RETRY_COUNT              1

#define NGX_HTTP_LIFECYCLE_KEEPALIVE_ACTION                 "keepalive"

#define NGX_HTTP_LIFECYCLE_MAX_READ_FILE_SIZE               (512 * 1024)
#define NGX_HTTP_LIFECYCLE_MAX_SIZE                         (ULLONG_MAX - 1)

#define NGX_HTTP_LIFECYCLE_ZERO_BUF_SIZE                    (512 * 1024)

#define NGX_HTTP_LIFECYCLE_INVALID_SERVER_ID                0

#define NGX_HTTP_LIFECYCLE_FILE_NAME_V1_LEN                  18
#define NGX_HTTP_LIFECYCLE_FILE_NAME_V2_LEN                  26
#define NGX_HTTP_LIFECYCLE_FILE_NAME_BUFF_LEN                27
#define NGX_HTTP_LIFECYCLE_MAX_FILE_NAME_LEN                 256
#define NGX_HTTP_LIFECYCLE_MAX_SUFFIX_LEN                    109 /* 128 - 19 */
#define NGX_HTTP_LIFECYCLE_MAX_APPKEY_LEN                    64

//TODO: set to the appropriate count
#define NGX_HTTP_LIFECYCLE_MAX_MEMBER_COUNT                  8


#define NGX_HTTP_LIFECYCLE_GMT_TIME_SIZE                     (sizeof("Mon, 28 Sep 1970 06:00:00 GMT") - 1)

#define NGX_HTTP_LIFECYCLE_MAX_FRAGMENT_SIZE                 (2 * 1024 * 1024)
/* TODO: revisit this */
#define NGX_HTTP_LIFECYCLE_MAX_RESERVE_PREDATA_SIZE          (1 * 1024 * 1024)
#define NGX_HTTP_LIFECYCLE_DEFAULT_BODY_BUFFER_SIZE          (NGX_HTTP_LIFECYCLE_MAX_FRAGMENT_SIZE + NGX_HTTP_LIFECYCLE_MAX_RESERVE_PREDATA_SIZE)


#define NGX_HTTP_LIFECYCLE_MUR_HASH_SEED                     97

#define NGX_HTTP_LIFECYCLE_CLIENT_VERSION                    "NGINX"

#define NGX_HTTP_LIFECYCLE_MIN_TIMER_DELAY                    1000


/* 4 bytes version before data */
#define NGX_HTTP_LIFECYCLE_RESERVE_PREDATA_SIZE               4


/* copy from Amazon S3 doc
  The PUT request header is limited to 8 KB in size.
  Within the PUT request header, the user-defined metadata is limited to 2 KB in size.
*/
#define NGX_HTTP_LIFECYCLE_MAX_CUSTOMIZE_INFO_SIZE            2048

#define NGX_HTTP_LIFECYCLE_LOG_DELIMITER                      7

#define NGX_BSWAP_64(x)                         \
    ((((x) & 0xff00000000000000ull) >> 56)      \
    | (((x) & 0x00ff000000000000ull) >> 40)     \
    | (((x) & 0x0000ff0000000000ull) >> 24)     \
    | (((x) & 0x000000ff00000000ull) >> 8)      \
    | (((x) & 0x00000000ff000000ull) << 8)      \
    | (((x) & 0x0000000000ff0000ull) << 24)     \
    | (((x) & 0x000000000000ff00ull) << 40)     \
    | (((x) & 0x00000000000000ffull) << 56))

#define ngx_http_lifecycle_clear_buf(b) \
    b->pos = b->start;            \
    b->last = b->start;

#if (NGX_HAVE_BIG_ENDIAN)

#define ngx_hton64(x) x

#define ngx_ntoh64(x) x

#else

#define ngx_hton64(x)                           \
    NGX_BSWAP_64(x)

#define ngx_ntoh64(x)                           \
    NGX_BSWAP_64(x)


#endif

typedef struct ngx_http_lifecycle_s ngx_http_lifecycle_t;
typedef struct ngx_http_lifecycle_peer_connection_s ngx_http_lifecycle_peer_connection_t;

typedef struct ngx_http_lifecycle_inet_s ngx_http_lifecycle_inet_t;



typedef struct {
    uint32_t     crc;
    uint32_t     data_crc;
} ngx_http_lifecycle_crc_t;

typedef struct {
    uint16_t             code;
    ngx_str_t            msg;
} ngx_http_lifecycle_action_t;


struct ngx_http_lifecycle_inet_s {
    uint32_t       ip;
    uint32_t       port;
};


typedef enum
{
    NGX_HTTP_LIFECYCLE_RELATIVE_TIME = 1,
    NGX_HTTP_LIFECYCLE_POSITIVE_TIME
} ngx_http_lifecycle_expire_time_type_e;


typedef enum
{
    NGX_HTTP_LIFECYCLE_CUSTOM_FT_FILE = 1,
    NGX_HTTP_LIFECYCLE_CUSTOM_FT_DIR,
    NGX_HTTP_LIFECYCLE_CUSTOM_FT_PWRITE_FILE
} ngx_http_lifecycle_custom_file_type_e;


typedef enum {
    NGX_HTTP_TFS_FILE_INFO_CLUSTER_ID_TAG = 101,
    NGX_HTTP_TFS_FILE_INFO_BLOCK_ID_TAG = 102,
    NGX_HTTP_TFS_FILE_INFO_FILE_ID_TAG = 103,
    NGX_HTTP_TFS_FILE_INFO_OFFSET_TAG = 104,
    NGX_HTTP_TFS_FILE_INFO_FILE_SIZE_TAG = 105,

    NGX_HTTP_TFS_OBJECT_META_INFO_CREATE_TIME_TAG = 201,
    NGX_HTTP_TFS_OBJECT_META_INFO_MODIFY_TIME_TAG = 202,
    NGX_HTTP_TFS_OBJECT_META_INFO_MAX_TFS_FILE_SIZE_TAG = 203,
    NGX_HTTP_TFS_OBJECT_META_INFO_BIG_FILE_SIZE_TAG = 204,
    NGX_HTTP_TFS_OBJECT_META_INFO_OWNER_ID_TAG = 205,

    NGX_HTTP_TFS_CUSTOMIZE_INFO_OTAG_TAG = 301,

    NGX_HTTP_TFS_OBJECT_INFO_HAS_META_INFO_TAG = 401,
    NGX_HTTP_TFS_OBJECT_INFO_HAS_CUSTOMIZE_INFO_TAG = 402,
    NGX_HTTP_TFS_OBJECT_INFO_META_INFO_TAG = 403,
    NGX_HTTP_TFS_OBJECT_INFO_V_TFS_FILE_INFO_TAG = 404,
    NGX_HTTP_TFS_OBJECT_INFO_CUSTOMIZE_INFO_TAG = 405,

    NGX_HTTP_TFS_BUCKET_META_INFO_CREATE_TIME_TAG = 501,
    NGX_HTTP_TFS_BUCKET_META_INFO_OWNER_ID_TAG = 502,

    NGX_HTTP_LIFECYCLE_KV_META_TABLE_V_META_TABLE_TAG = 601,

    NGX_HTTP_TFS_BLOCK_CACHE_KEY_NS_ADDR_TAG = 701,
    NGX_HTTP_TFS_BLOCK_CACHE_KEY_BLOCK_ID_TAG = 702,
    NGX_HTTP_TFS_BLOCK_CACHE_VALUE_DS_LIST_TAG = 703,
    NGX_HTTP_TFS_BLOCK_CACHE_VALUE_FAMILY_INFO_TAG = 704,

    NGX_HTTP_TFS_USER_INFO_OWNER_ID_TAG = 801,

    NGX_HTTP_LIFECYCLE_END_TAG = 999
} ngx_http_tfs_identify_tag_e;

typedef struct {
    ngx_http_lifecycle_inet_t    table[NGX_HTTP_LIFECYCLE_METASERVER_COUNT];
    uint32_t                     size;
    ngx_flag_t                   valid;
} ngx_http_lifecycle_kv_meta_table_t;



typedef enum
{
    NGX_HTTP_LIFECYCLE_ACTION_NON = 0,
    NGX_HTTP_LIFECYCLE_ACTION_POST = 1,
    NGX_HTTP_LIFECYCLE_ACTION_PUT = 2,
    NGX_HTTP_LIFECYCLE_ACTION_GET = 3,
    NGX_HTTP_LIFECYCLE_ACTION_DEL = 4,

} ngx_http_lifecycle_action_e;


typedef enum {
    NGX_HTTP_LIFECYCLE_ROOT_SERVER = 0,
    NGX_HTTP_LIFECYCLE_META_SERVER,
} ngx_http_lifecycle_peer_server_e;


typedef enum {
    NGX_HTTP_LIFECYCLE_STATE_ACTION_GET_META_TABLE = 0,
    NGX_HTTP_LIFECYCLE_STATE_ACTION_PROCESS,
    NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE,
} ngx_http_lifecycle_state_action_e;



static inline uint32_t
ngx_http_lifecycle_crc(uint32_t crc, const char *data, size_t len)
{
    size_t i;

    for (i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ ngx_crc32_table256[(crc ^ *data++) & 0xff];
    }

    return crc;
}

ngx_chain_t *ngx_http_lifecycle_alloc_chains(ngx_pool_t *pool, size_t count);
void ngx_http_lifecycle_free_chains(ngx_chain_t **free, ngx_chain_t **out);

ngx_int_t ngx_http_lifecycle_test_connect(ngx_connection_t *c);
uint64_t ngx_http_lifecycle_generate_packet_id(void);

ngx_int_t ngx_http_lifecycle_parse_headerin(ngx_http_request_t *r,
    ngx_str_t *header_name, ngx_str_t *value);

ngx_int_t ngx_http_lifecycle_compute_buf_crc(ngx_http_lifecycle_crc_t *t_crc, ngx_buf_t *b,
    size_t size, ngx_log_t *log);

ngx_int_t ngx_http_lifecycle_peer_set_addr(ngx_pool_t *pool,
    ngx_http_lifecycle_peer_connection_t *p, ngx_http_lifecycle_inet_t *addr);
u_char *ngx_http_lifecycle_get_peer_addr_text(ngx_http_lifecycle_inet_t *addr);

uint32_t ngx_http_lifecycle_murmur_hash(u_char *data, size_t len);

ngx_int_t ngx_http_lifecycle_parse_inet(ngx_str_t *u, ngx_http_lifecycle_inet_t *addr);
int32_t ngx_http_lifecycle_raw_fsname_hash(const u_char *str, const int32_t len);
ngx_int_t ngx_http_lifecycle_get_local_ip(ngx_str_t device, struct sockaddr_in *addr);
ngx_buf_t *ngx_http_lifecycle_copy_buf_chain(ngx_pool_t *pool, ngx_chain_t *in);
ngx_int_t ngx_http_lifecycle_sum_md5(ngx_chain_t *body, u_char *md5_final, ssize_t *body_size, ngx_log_t *log);
u_char *ngx_http_lifecycle_time(u_char *buf, time_t t);
ngx_int_t ngx_http_lifecycle_get_positive_time(ngx_str_t *buf);
ngx_int_t ngx_http_lifecycle_get_relative_time(ngx_str_t *buf);
ngx_int_t ngx_http_lifecycle_get_mk_time(ngx_str_t *buf);

ngx_int_t ngx_http_lifecycle_status_message(ngx_buf_t *b, ngx_str_t *action, ngx_log_t *log);
ngx_int_t ngx_http_lifecycle_atoull(u_char *line, size_t n, unsigned long long *value);
uint64_t ngx_http_lifecycle_get_chain_buf_size(ngx_chain_t *data);

char *positive_expiretime_str;
char *relative_expiretime_str;
char *lifecycle_uri_prefix;


#define ngx_http_lifecycle_free_st(t)           \
        t->next = t->parent->free_sts;   \
        t->parent->free_sts = t;         \

#define ngx_http_lifecycle_get_data_member_count(x) ((x >> 24) & 0xFF)
#define ngx_http_lifecycle_get_check_member_count(x) ((x >> 16) & 0xFF)


#endif  /* _NGX_HTTP_LIFECYCLE_COMMON_H_INCLUDED_ */
