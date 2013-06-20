
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#ifndef _NGX_HTTP_TFS_COMMON_H_INCLUDED_
#define _NGX_HTTP_TFS_COMMON_H_INCLUDED_


#include <ngx_core.h>
#include <ngx_http.h>
#define NGX_PACKED __attribute__ ((__packed__))

#define NGX_HTTP_TFS_HEADER                           0
#define NGX_HTTP_TFS_BODY                             1
#define NGX_HTTP_TFS_DEFAULT_BODY_BUFFER_SIZE         (2 * 1024 * 1024)

#define NGX_HTTP_TFS_YES                              1
#define NGX_HTTP_TFS_NO                               0
#define NGX_HTTP_TFS_AGAIN                            -20

#define NGX_HTTP_TFS_NGINX_APPKEY                     "tfscom"
#define NGX_HTTP_TFS_RCS_LOCK_FILE                    "nginx_rcs.lock"

#define NGX_HTTP_TFS_MD5_RESULT_LEN                   16
#define NGX_HTTP_TFS_DUPLICATE_KEY_SIZE               (sizeof(uint32_t) + NGX_HTTP_TFS_MD5_RESULT_LEN)
#define NGX_HTTP_TFS_DUPLICATE_VALUE_BASE_SIZE        sizeof(int32_t)
#define NGX_HTTP_TFS_DUPLICATE_INITIAL_MAGIC_VERSION  0x0fffffff

#define NGX_HTTP_TFS_SERVER_COUNT                     5            /* rcs, ns, ds, rs, ms */
#define NGX_HTTP_TFS_METASERVER_COUNT                 1024
#define NGX_HTTP_TFS_TAIR_SERVER_ADDR_PART_COUNT      3            /* master_conifg_server;slave_config_server;group */
#define NGX_HTTP_TFS_TAIR_CONFIG_SERVER_COUNT         2            /* master && slave */

#define NGX_HTTP_TFS_KV_META_RETRY_COUNT              1

#define NGX_HTTP_TFS_KEEPALIVE_ACTION                 "keepalive"

#define NGX_HTTP_TFS_MAX_READ_FILE_SIZE               (512 * 1024)
#define NGX_HTTP_TFS_USE_LARGE_FILE_SIZE              (15 * 1024 * 1024)
#define NGX_HTTP_TFS_MAX_SIZE                         (ULLONG_MAX - 1)

#define NGX_HTTP_TFS_ZERO_BUF_SIZE                    (512 * 1024)
#define NGX_HTTP_TFS_INIT_FILE_HOLE_COUNT             5

#define NGX_HTTP_TFS_APPEND_OFFSET                     -1

/* tfs file name standard name length */
#define NGX_HTTP_TFS_SMALL_FILE_KEY_CHAR              'T'
#define NGX_HTTP_TFS_LARGE_FILE_KEY_CHAR              'L'
#define NGX_HTTP_TFS_FILE_NAME_LEN                     18
#define NGX_HTTP_TFS_FILE_NAME_BUFF_LEN                19
#define NGX_HTTP_TFS_FILE_NAME_EXCEPT_SUFFIX_LEN       12
#define NGX_HTTP_TFS_MAX_FILE_NAME_LEN                 256
#define NGX_HTTP_TFS_MAX_SUFFIX_LEN                    109 /* 128 - 19 */
#define NGX_HTTP_TFS_MAX_BUCKET_NAME_LEN               256
#define NGX_HTTP_TFS_MAX_OBJECT_NAME_LEN               256


#define NGX_HTTP_TFS_MAX_CLUSTER_COUNT                 10
#define NGX_HTTP_TFS_MAX_CLUSTER_ID_COUNT              5

#define NGX_HTTP_TFS_CMD_GET_CLUSTER_ID_NS             20
#define NGX_HTTP_TFS_CMD_GET_GROUP_COUNT               22
#define NGX_HTTP_TFS_CMD_GET_GROUP_SEQ                 23

#define NGX_HTTP_TFS_GMT_TIME_SIZE                     (sizeof("Mon, 28 Sep 1970 06:00:00 GMT") - 1)

#define NGX_HTTP_TFS_MAX_FRAGMENT_SIZE                 (2 * 1024 * 1024)
#define NGX_HTTP_TFS_MAX_BATCH_COUNT                   8


#define NGX_HTTP_TFS_MUR_HASH_SEED                     97

#define NGX_HTTP_TFS_CLIENT_VERSION                    "NGINX"

#define NGX_HTTP_TFS_MIN_TIMER_DELAY                    1000

#define NGX_HTTP_TFS_READ_STAT_NORMAL                   0
#define NGX_HTTP_TFS_READ_STAT_FORCE                    1

#define NGX_HTTP_TFS_BUCKET_META_INFO_TAG_SIZE          (5 * sizeof(uint32_t))
#define NGX_HTTP_TFS_USER_INFO_TAG_SIZE                 (2 * sizeof(uint32_t))
#define NGX_HTTP_TFS_OBJECT_META_INFO_TAG_SIZE          (6 * sizeof(uint32_t))
#define NGX_HTTP_TFS_CUSTOMIZE_INFO_TAG_SIZE            (2 * sizeof(uint32_t))
#define NGX_HTTP_TFS_FILE_INFO_TAG_SIZE                 (6 * sizeof(uint32_t))

#define NGX_HTTP_TFS_GET_BUCKET_MAX_KEYS                1000
#define NGX_HTTP_TFS_GET_BUCKET_DEFAULT_DELIMITER       '/'

/* copy from Amazon S3 doc
  The PUT request header is limited to 8 KB in size.
  Within the PUT request header, the user-defined metadata is limited to 2 KB in size.
*/
#define NGX_HTTP_TFS_MAX_CUSTOMIZE_INFO_SIZE            2048

#define NGX_HTTP_TFS_LOG_DELIMITER                      7


#define NGX_BSWAP_64(x)                         \
    ((((x) & 0xff00000000000000ull) >> 56)      \
    | (((x) & 0x00ff000000000000ull) >> 40)     \
    | (((x) & 0x0000ff0000000000ull) >> 24)     \
    | (((x) & 0x000000ff00000000ull) >> 8)      \
    | (((x) & 0x00000000ff000000ull) << 8)      \
    | (((x) & 0x0000000000ff0000ull) << 24)     \
    | (((x) & 0x000000000000ff00ull) << 40)     \
    | (((x) & 0x00000000000000ffull) << 56))

#define ngx_http_tfs_clear_buf(b) \
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

typedef struct ngx_http_tfs_s ngx_http_tfs_t;
typedef struct ngx_http_tfs_peer_connection_s ngx_http_tfs_peer_connection_t;

typedef struct ngx_http_tfs_inet_s ngx_http_tfs_inet_t;
typedef struct ngx_http_tfs_meta_hh_s  ngx_http_tfs_meta_hh_t;

typedef struct ngx_http_tfs_segment_data_s ngx_http_tfs_segment_data_t;

typedef struct ngx_http_tfs_custom_file_info_chain_s ngx_http_tfs_custom_file_info_chain_t;
typedef struct ngx_http_tfs_object_meta_info_chain_s ngx_http_tfs_object_meta_info_chain_t;

typedef struct {
    ngx_str_t           access_secret_key;
    ngx_str_t           user_name;
} NGX_PACKED ngx_http_tfs_authorize_info_t;


typedef struct {
    uint64_t           size;
} ngx_http_tfs_stat_info_t;


typedef struct {
    uint32_t     crc;
    uint32_t     data_crc;
} ngx_http_tfs_crc_t;


typedef struct {
    uint16_t             code;
    ngx_str_t            msg;
} ngx_http_tfs_action_t;


struct ngx_http_tfs_meta_hh_s {
    uint64_t       app_id;
    uint64_t       user_id;
};


struct ngx_http_tfs_inet_s {
    uint32_t       ip;
    uint32_t       port;
};


typedef struct {
    uint64_t                   id;
    int32_t                    offset;
    int32_t                    size;
    int32_t                    u_size;
    int32_t                    modify_time;
    int32_t                    create_time;
    int32_t                    flag;
    uint32_t                   crc;
} NGX_PACKED ngx_http_tfs_raw_file_info_t;


typedef struct {
    ngx_str_t                  name;
    int64_t                    pid;
    int64_t                    id;
    uint32_t                   create_time;
    uint32_t                   modify_time;
    uint64_t                   size;
    uint16_t                   ver_no;
    uint64_t                   owner_id;
} NGX_PACKED ngx_http_tfs_custom_file_info_t;


struct ngx_http_tfs_custom_file_info_chain_s {
    uint32_t                                file_count;
    ngx_http_tfs_custom_file_info_t        *file_info;
    ngx_http_tfs_custom_file_info_chain_t  *next;
};


typedef struct {
    uint64_t                               offset;
    uint64_t                               length;
} ngx_http_tfs_file_hole_info_t;


typedef struct {
    ngx_http_tfs_inet_t          table[NGX_HTTP_TFS_METASERVER_COUNT];
    uint32_t                     size;
    ngx_flag_t                   valid;
} ngx_http_tfs_kv_meta_table_t;


typedef struct {
    int64_t           create_time;
    uint64_t          owner_id;
    int8_t            has_tag_info;
    uint32_t          acl_size;
} NGX_PACKED ngx_http_tfs_bucket_meta_info_t;


typedef struct {
    int64_t           create_time;
    int64_t           modify_time;
    uint32_t          max_tfs_file_size;
    uint64_t          big_file_size;
    uint64_t          owner_id;
} NGX_PACKED ngx_http_tfs_object_meta_info_t;


typedef struct {
    uint32_t          otag_len;
    u_char           *otag;
} NGX_PACKED ngx_http_tfs_customize_info_t;


typedef struct {
    int32_t            cluster_id;
    uint64_t           block_id;
    uint64_t           file_id;
    int64_t            offset;
    uint64_t           size;
} NGX_PACKED ngx_http_tfs_file_info_t;


typedef struct {
    uint8_t                          has_meta_info;
    uint8_t                          has_customize_info;
    ngx_http_tfs_object_meta_info_t  meta_info;
    ngx_http_tfs_customize_info_t    customize_info;
    uint32_t                         tfs_file_count;
    ngx_http_tfs_file_info_t        *tfs_file_infos;
} NGX_PACKED ngx_http_tfs_object_info_t;


struct ngx_http_tfs_object_meta_info_chain_s {
    uint32_t                                object_count;
    ngx_http_tfs_object_meta_info_t        *meta_info;
    ngx_http_tfs_object_meta_info_chain_t  *next;
};


typedef struct {
    ngx_str_t                               prefix;
    ngx_str_t                               marker;
    uint32_t                                max_keys;
    uint8_t                                 delimiter;
    uint32_t                                common_prefix_count;
    ngx_str_t                              *common_prefix;
    uint32_t                                object_name_count;
    ngx_str_t                              *object_name;
    ngx_http_tfs_object_meta_info_chain_t  *object_meta_infos;
    ngx_http_tfs_custom_file_info_chain_t  *custom_file_infos;
} NGX_PACKED ngx_http_tfs_get_bucket_ctx_t;


#define ngx_http_tfs_customize_info_size(customize_info)                       \
    (sizeof(uint32_t) + (customize_info)->otag_len + 1 +                       \
     NGX_HTTP_TFS_CUSTOMIZE_INFO_TAG_SIZE)

// TODO: turn to function
#define ngx_http_tfs_object_info_size(object_info)                             \
    (2 * sizeof(uint8_t) + 4 * sizeof(uint32_t) +                              \
     sizeof(uint32_t) +                                                        \
     (object_info)->tfs_file_count * (sizeof(ngx_http_tfs_file_info_t) +       \
                                      NGX_HTTP_TFS_FILE_INFO_TAG_SIZE) +       \
     ((object_info)->has_meta_info ?                                           \
       (sizeof(ngx_http_tfs_object_meta_info_t) +                              \
        sizeof(uint32_t) + NGX_HTTP_TFS_OBJECT_META_INFO_TAG_SIZE) : 0) +      \
     ((object_info)->has_customize_info ?                                      \
       (sizeof(uint32_t) +                                                     \
       ngx_http_tfs_customize_info_size(&(object_info)->customize_info)) : 0))


typedef struct {
    uint64_t           owner_id;
} NGX_PACKED ngx_http_tfs_user_info_t;


typedef enum
{
    NGX_HTTP_TFS_ACTION_NON = 0,
    NGX_HTTP_TFS_ACTION_CREATE_DIR = 1,
    NGX_HTTP_TFS_ACTION_CREATE_FILE = 2,
    NGX_HTTP_TFS_ACTION_REMOVE_DIR = 3,
    NGX_HTTP_TFS_ACTION_MOVE_DIR = 5,
    NGX_HTTP_TFS_ACTION_MOVE_FILE = 6,
    NGX_HTTP_TFS_ACTION_KEEPALIVE = 12,
    NGX_HTTP_TFS_ACTION_GET_APPID = 13,
    NGX_HTTP_TFS_ACTION_PUT_BUCKET = 14,
    NGX_HTTP_TFS_ACTION_GET_BUCKET = 15,
    NGX_HTTP_TFS_ACTION_DEL_BUCKET = 16,
    NGX_HTTP_TFS_ACTION_HEAD_BUCKET = 17,
    NGX_HTTP_TFS_ACTION_PUT_OBJECT = 18,
    NGX_HTTP_TFS_ACTION_GET_OBJECT = 19,
    NGX_HTTP_TFS_ACTION_DEL_OBJECT = 20,
    NGX_HTTP_TFS_ACTION_HEAD_OBJECT = 21,
    NGX_HTTP_TFS_ACTION_UNDELETE_FILE = 22,
    NGX_HTTP_TFS_ACTION_CONCEAL_FILE = 23,
    NGX_HTTP_TFS_ACTION_REVEAL_FILE = 24,
} ngx_http_tfs_action_e;


typedef enum {
    NGX_HTTP_TFS_RC_SERVER = 0,
    NGX_HTTP_TFS_NAME_SERVER,
    NGX_HTTP_TFS_DATA_SERVER,
    NGX_HTTP_TFS_ROOT_SERVER,
    NGX_HTTP_TFS_META_SERVER,
} ngx_http_tfs_peer_server_e;


typedef enum {
    NGX_HTTP_TFS_STATE_WRITE_START = 0,
    NGX_HTTP_TFS_STATE_WRITE_GET_META_TABLE,
    NGX_HTTP_TFS_STATE_WRITE_GET_AUTH,
    NGX_HTTP_TFS_STATE_WRITE_GET_CLUSTER_ID,
    NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO,
    NGX_HTTP_TFS_STATE_WRITE_STAT_DUP_FILE,
    NGX_HTTP_TFS_STATE_WRITE_CREATE_FILE_NAME,
    NGX_HTTP_TFS_STATE_WRITE_WRITE_DATA,
    NGX_HTTP_TFS_STATE_WRITE_CLOSE_FILE,
    NGX_HTTP_TFS_STATE_WRITE_PUT_OBJECT_INFO,
    NGX_HTTP_TFS_STATE_WRITE_DONE,
    NGX_HTTP_TFS_STATE_WRITE_DELETE_DATA,
} ngx_http_tfs_state_write_e;


typedef enum {
    NGX_HTTP_TFS_STATE_READ_START = 0,
    NGX_HTTP_TFS_STATE_READ_GET_META_TABLE,
    NGX_HTTP_TFS_STATE_READ_GET_AUTH,
    NGX_HTTP_TFS_STATE_READ_GET_OBJECT_INFO,
    NGX_HTTP_TFS_STATE_READ_GET_BLK_INFO,
    NGX_HTTP_TFS_STATE_READ_READ_DATA,
    NGX_HTTP_TFS_STATE_READ_DONE,
} ngx_http_tfs_state_read_e;


typedef enum {
    NGX_HTTP_TFS_STATE_REMOVE_START = 0,
    NGX_HTTP_TFS_STATE_REMOVE_GET_META_TABLE,
    NGX_HTTP_TFS_STATE_REMOVE_GET_AUTH,
    NGX_HTTP_TFS_STATE_REMOVE_DEL_OBJECT_INFO,
    NGX_HTTP_TFS_STATE_REMOVE_GET_GROUP_COUNT,
    NGX_HTTP_TFS_STATE_REMOVE_GET_GROUP_SEQ,
    NGX_HTTP_TFS_STATE_REMOVE_GET_BLK_INFO,
    NGX_HTTP_TFS_STATE_REMOVE_STAT_FILE,
    NGX_HTTP_TFS_STATE_REMOVE_READ_META_SEGMENT,
    NGX_HTTP_TFS_STATE_REMOVE_DELETE_DATA,
    NGX_HTTP_TFS_STATE_REMOVE_DONE,
} ngx_http_tfs_state_remove_e;


typedef enum {	
    NGX_HTTP_TFS_STATE_STAT_START = 0,
    NGX_HTTP_TFS_STATE_STAT_GET_BLK_INFO,
    NGX_HTTP_TFS_STATE_STAT_STAT_FILE,
    NGX_HTTP_TFS_STATE_STAT_DONE,
} ngx_http_tfs_state_stat_e;


typedef enum {
    NGX_HTTP_TFS_STATE_ACTION_START = 0,
    NGX_HTTP_TFS_STATE_ACTION_GET_META_TABLE,
    NGX_HTTP_TFS_STATE_ACTION_GET_AUTH,
    NGX_HTTP_TFS_STATE_ACTION_PROCESS,
    NGX_HTTP_TFS_STATE_ACTION_DONE,
} ngx_http_tfs_state_action_e;


typedef enum
{
    NGX_HTTP_TFS_UNLINK_DELETE = 0,
    NGX_HTTP_TFS_UNLINK_UNDELETE = 2,	
    NGX_HTTP_TFS_UNLINK_CONCEAL = 4,	
    NGX_HTTP_TFS_UNLINK_REVEAL = 6
} ngx_http_tfs_unlink_type_e;


typedef enum
{
    NGX_HTTP_TFS_FILE_NORMAL = 0,
    NGX_HTTP_TFS_FILE_DELETED = 1,
    NGX_HTTP_TFS_FILE_INVALID = 2,	
    NGX_HTTP_TFS_FILE_CONCEAL = 4
} ngx_http_tfs_file_status_e;


typedef enum
{
    NGX_HTTP_TFS_CUSTOM_FT_FILE = 1,
    NGX_HTTP_TFS_CUSTOM_FT_DIR,
    NGX_HTTP_TFS_CUSTOM_FT_PWRITE_FILE
} ngx_http_tfs_custom_file_type_e;


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
    NGX_HTTP_TFS_BUCKET_META_INFO_HAS_TAG_INFO_TAG = 503,
    NGX_HTTP_TFS_BUCKET_META_INFO_BUCKET_ACL_MAP_TAG = 505,

    NGX_HTTP_TFS_KV_META_TABLE_V_META_TABLE_TAG = 601,

    NGX_HTTP_TFS_BLOCK_CACHE_DS_LIST_TAG = 701,
    NGX_HTTP_TFS_BLOCK_CACHE_FAMILY_INFO_TAG = 702,

    NGX_HTTP_TFS_USER_INFO_OWNER_ID_TAG = 801,

    NGX_HTTP_TFS_END_TAG = 999
} ngx_http_tfs_identify_tag_e;


static inline uint32_t
ngx_http_tfs_crc(uint32_t crc, const char *data, size_t len)
{
    size_t i;

    for (i = 0; i < len; ++i) {
        crc = (crc >> 8) ^ ngx_crc32_table256[(crc ^ *data++) & 0xff];
    }

    return crc;
}

ngx_chain_t *ngx_http_tfs_alloc_chains(ngx_pool_t *pool, size_t count);
void ngx_http_tfs_free_chains(ngx_chain_t **free, ngx_chain_t **out);

ngx_int_t ngx_http_tfs_test_connect(ngx_connection_t *c);
uint64_t ngx_http_tfs_generate_packet_id(void);

ngx_int_t ngx_http_tfs_parse_headerin(ngx_http_request_t *r,
    ngx_str_t *header_name, ngx_str_t *value);

ngx_int_t ngx_http_tfs_compute_buf_crc(ngx_http_tfs_crc_t *t_crc, ngx_buf_t *b,
    size_t size, ngx_log_t *log);

ngx_int_t ngx_http_tfs_peer_set_addr(ngx_pool_t *pool,
    ngx_http_tfs_peer_connection_t *p, ngx_http_tfs_inet_t *addr);

uint32_t ngx_http_tfs_murmur_hash(u_char *data, size_t len);

ngx_int_t ngx_http_tfs_parse_inet(ngx_str_t *u, ngx_http_tfs_inet_t *addr);
int32_t ngx_http_tfs_raw_fsname_hash(const u_char *str, const int32_t len);
ngx_int_t ngx_http_tfs_get_local_ip(ngx_str_t device, struct sockaddr_in *addr);
ngx_buf_t *ngx_http_tfs_copy_buf_chain(ngx_pool_t *pool, ngx_chain_t *in);
ngx_int_t ngx_http_tfs_sum_md5(ngx_chain_t *body, u_char *md5_final, ssize_t *body_size, ngx_log_t *log);
u_char *ngx_http_tfs_time(u_char *buf, time_t t);

ngx_int_t ngx_http_tfs_status_message(ngx_buf_t *b, ngx_str_t *action, ngx_log_t *log);
ngx_int_t ngx_http_tfs_get_parent_dir(ngx_str_t *file_path, ngx_int_t *dir_level);
ngx_int_t ngx_http_tfs_set_output_file_name(ngx_http_tfs_t *t);
ngx_int_t ngx_http_tfs_atoull(u_char *line, size_t n, unsigned long long *value);
uint64_t ngx_http_tfs_get_chain_buf_size(ngx_chain_t *data);

void ngx_http_tfs_dump_segment_data(ngx_http_tfs_segment_data_t *segment, ngx_log_t *log);
void ngx_http_tfs_dump_tfs_file_info(ngx_http_tfs_file_info_t *tfs_file_info, ngx_log_t *log);
void ngx_http_tfs_dump_object_info(ngx_http_tfs_object_info_t *object_info, ngx_log_t *log);
void ngx_http_tfs_dump_custom_file_info(ngx_http_tfs_custom_file_info_t *file_info, ngx_log_t *log);
void ngx_http_tfs_dump_object_meta_info(ngx_http_tfs_object_meta_info_t *meta_info, ngx_log_t *log);

ngx_http_tfs_t *ngx_http_tfs_alloc_st(ngx_http_tfs_t *t);

#define ngx_http_tfs_free_st(t)           \
        t->next = t->parent->free_sts;   \
        t->parent->free_sts = t;         \

ngx_int_t ngx_http_tfs_name_to_kv_init_meta(ngx_str_t *ctx_bucket_name,
    ngx_str_t *ctx_object_name, ngx_str_t *bucket_name, ngx_str_t *object_name);


#endif  /* _NGX_HTTP_TFS_COMMON_H_INCLUDED_ */
