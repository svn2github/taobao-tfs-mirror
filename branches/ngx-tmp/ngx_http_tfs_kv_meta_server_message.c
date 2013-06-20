
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#include <ngx_http_tfs_serialization.h>
#include <ngx_http_tfs_kv_meta_server_message.h>
#include <ngx_http_tfs_json.h>
#include <ngx_http_tfs_protocol.h>
#include <ngx_http_tfs_errno.h>

static ngx_chain_t *ngx_http_tfs_create_put_bucket_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_get_bucket_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_del_bucket_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_head_bucket_message(ngx_http_tfs_t *t);

static ngx_chain_t *ngx_http_tfs_create_put_object_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_get_object_message(ngx_http_tfs_t *t,
    int64_t offset, uint64_t length);
static ngx_chain_t *ngx_http_tfs_create_del_object_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_head_object_message(ngx_http_tfs_t *t);
static ngx_chain_t *ngx_http_tfs_create_get_authorize_message(ngx_http_tfs_t *t);

static ngx_int_t ngx_http_tfs_parse_put_bucket_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_get_bucket_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_del_bucket_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_head_bucket_message(ngx_http_tfs_t *t);

static ngx_int_t ngx_http_tfs_parse_put_object_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_get_object_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_del_object_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_head_object_message(ngx_http_tfs_t *t);
static ngx_int_t ngx_http_tfs_parse_get_authorize_message(ngx_http_tfs_t *t);

ngx_http_tfs_inet_t *
ngx_http_tfs_select_meta_server(ngx_http_tfs_t *t)
{
    uint32_t  index;

    if (t->kv_meta_retry++ > NGX_HTTP_TFS_KV_META_RETRY_COUNT) {
        return NULL;
    }

    index = ngx_random() % t->loc_conf->meta_server_table.size;

    return &(t->loc_conf->meta_server_table.table[index]);
}


ngx_chain_t *
ngx_http_tfs_meta_auth_create_message(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = NULL;
    cl = ngx_http_tfs_create_get_authorize_message(t);

    return cl;
}


ngx_int_t
ngx_http_tfs_meta_auth_parse_message(ngx_http_tfs_t *t)
{
    return ngx_http_tfs_parse_get_authorize_message(t);
}


static ngx_chain_t *
ngx_http_tfs_create_get_authorize_message(ngx_http_tfs_t *t)
{
    size_t                                       size;
    ngx_buf_t                                   *b;
    ngx_chain_t                                 *cl;
    ngx_http_tfs_kv_ms_get_authorize_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_get_authorize_request_t) +
        /* access key id */
        t->r_ctx.access_id.len + 1;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_get_authorize_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KV_GET_AUTHORIZE_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->access_key_id_len = t->r_ctx.access_id.len + 1;
    ngx_memcpy(req->access_key_id, t->r_ctx.access_id.data, t->r_ctx.access_id.len);



    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_int_t
ngx_http_tfs_parse_get_authorize_message(ngx_http_tfs_t *t)
{
    u_char                                       *p;
    uint16_t                                      type;
    ngx_int_t                                     rc;
    ngx_str_t                                     action;
    ngx_http_tfs_header_t                        *header;
    ngx_http_tfs_peer_connection_t               *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "get authorize (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    p = tp->body_buffer.pos;

    rc = ngx_http_tfs_deserialize_authorize_info(&p, t->pool, &t->authorize_info);

    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                  "access secret key is %V and username is %V",
                  &t->authorize_info.access_secret_key,
                  &t->authorize_info.user_name);

    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    return rc;
}


ngx_chain_t *
ngx_http_tfs_meta_server_create_message(ngx_http_tfs_t *t)
{
    uint16_t      action;
    ngx_chain_t  *cl;

    cl = NULL;
    action = t->r_ctx.action.code;

    switch (action) {

    case NGX_HTTP_TFS_ACTION_PUT_BUCKET:
        cl = ngx_http_tfs_create_put_bucket_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_GET_BUCKET:
    case NGX_HTTP_TFS_ACTION_REMOVE_DIR:
        cl = ngx_http_tfs_create_get_bucket_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_DEL_BUCKET:
        cl = ngx_http_tfs_create_del_bucket_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_HEAD_BUCKET:
        cl = ngx_http_tfs_create_head_bucket_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_CREATE_FILE:
    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        cl = ngx_http_tfs_create_put_object_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        cl = ngx_http_tfs_create_get_object_message(t, t->file.file_offset,
                                                    t->file.left_length);
        break;

    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        cl = ngx_http_tfs_create_del_object_message(t);
        break;

    case NGX_HTTP_TFS_ACTION_HEAD_OBJECT:
        cl = ngx_http_tfs_create_head_object_message(t);
        break;
    }

    return cl;
}


ngx_int_t
ngx_http_tfs_meta_server_parse_message(ngx_http_tfs_t *t)
{
    uint16_t  action;

    action = t->r_ctx.action.code;

    switch (action) {

    case NGX_HTTP_TFS_ACTION_PUT_BUCKET:
        return ngx_http_tfs_parse_put_bucket_message(t);

    case NGX_HTTP_TFS_ACTION_REMOVE_DIR:
    case NGX_HTTP_TFS_ACTION_GET_BUCKET:
        return ngx_http_tfs_parse_get_bucket_message(t);

    case NGX_HTTP_TFS_ACTION_DEL_BUCKET:
        return ngx_http_tfs_parse_del_bucket_message(t);

    case NGX_HTTP_TFS_ACTION_HEAD_BUCKET:
        return ngx_http_tfs_parse_head_bucket_message(t);

    case NGX_HTTP_TFS_ACTION_CREATE_FILE:
    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        return ngx_http_tfs_parse_put_object_message(t);

    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        return ngx_http_tfs_parse_get_object_message(t);

    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        return ngx_http_tfs_parse_del_object_message(t);

    case NGX_HTTP_TFS_ACTION_HEAD_OBJECT:
        return ngx_http_tfs_parse_head_object_message(t);

    default:
        return NGX_ERROR;
    }

    return NGX_ERROR;
}


static ngx_chain_t *
ngx_http_tfs_create_put_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_put_bucket_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_put_bucket_request_t) +
        t->file.bucket_name.len + 1 +
        /* bucket meta info */
        sizeof(ngx_http_tfs_bucket_meta_info_t) +
        NGX_HTTP_TFS_BUCKET_META_INFO_TAG_SIZE +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_put_bucket_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_PUT_BUCKET_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    rc = ngx_http_tfs_serialize_bucket_meta_info(&p, &t->file.bucket_meta_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }
    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_get_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_get_bucket_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_get_bucket_request_t) +
        t->file.bucket_name.len + 1 +
        /* prefix length */
        sizeof(uint32_t) +
        /* prefix */
        (t->get_bucket_ctx.prefix.len > 0 ? (t->get_bucket_ctx.prefix.len + 1) : 0) +
        /* marker length */
        sizeof(uint32_t) +
        /* marker */
        (t->get_bucket_ctx.marker.len > 0 ? (t->get_bucket_ctx.marker.len + 1) : 0) +
        /* max keys */
        sizeof(uint32_t) +
        /* delimiter */
        sizeof(uint8_t) +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_get_bucket_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_GET_BUCKET_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    /* prefix */
    rc = ngx_http_tfs_serialize_string(&p, &t->get_bucket_ctx.prefix);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    /* marker */
    rc = ngx_http_tfs_serialize_string(&p, &t->get_bucket_ctx.marker);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    /* max_keys */
    *((uint32_t *)p) = t->get_bucket_ctx.max_keys;
    p += sizeof(uint32_t);

    /* delimiter */
    *((uint8_t *)p) = t->get_bucket_ctx.delimiter;
    p += sizeof(uint8_t);

    /* user info */
    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_del_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_del_bucket_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_del_bucket_request_t) +
        t->file.bucket_name.len + 1 +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_del_bucket_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_DEL_BUCKET_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_head_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                    *p;
    size_t                                     size;
    ngx_int_t                                  rc;
    ngx_buf_t                                 *b;
    ngx_chain_t                               *cl;
    ngx_http_tfs_kv_ms_head_bucket_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_head_bucket_request_t) +
        t->file.bucket_name.len + 1 +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_head_bucket_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_HEAD_BUCKET_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_put_object_message(ngx_http_tfs_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_put_object_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_put_object_request_t) +
        t->file.bucket_name.len + 1 +
        /* object name len */
        sizeof(uint32_t) +
        /* object name */
        t->file.object_name.len + 1 +
        /* object info */
        ngx_http_tfs_object_info_size(&t->file.object_info) +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_put_object_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_PUT_OBJECT_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    /* object name */
    *((uint32_t *)p) = t->file.object_name.len + 1;
    p += sizeof(uint32_t);
    ngx_memcpy(p, t->file.object_name.data, t->file.object_name.len);
    p += t->file.object_name.len + 1;

#if (NGX_DEBUG)
    ngx_http_tfs_dump_object_info(&t->file.object_info, t->log);
#endif

    rc = ngx_http_tfs_serialize_object_info(&p, &t->file.object_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_get_object_message(ngx_http_tfs_t *t, int64_t offset,
    uint64_t length)
{
    u_char                                   *p;
    size_t                                    size, req_frag_count;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_get_object_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_get_object_request_t) +
        t->file.bucket_name.len + 1 +
        /* object name len */
        sizeof(uint32_t) +
        /* object name */
        t->file.object_name.len + 1 +
        /* offset && length */
        sizeof(int64_t) * 2 +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_get_object_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_GET_OBJECT_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    /* object name */
    *((uint32_t *)p) = t->file.object_name.len + 1;
    p += sizeof(uint32_t);
    ngx_memcpy(p, t->file.object_name.data, t->file.object_name.len);
    p += t->file.object_name.len + 1;

    /* offset */
    *((int64_t *)p) = offset;
    p += sizeof(int64_t);

    /* length */
    req_frag_count = length / (NGX_HTTP_TFS_MAX_FRAGMENT_SIZE);

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, t->log, 0 ,"max_frag_count: %uz, req_frag_count: %uz, data size: %uz",
                   t->max_frag_count, req_frag_count, length);

    if (req_frag_count > t->max_frag_count) {
        *((uint64_t *) p) = (t->max_frag_count - 1) * NGX_HTTP_TFS_MAX_FRAGMENT_SIZE;
        p += sizeof(uint64_t);
        t->has_split_frag = NGX_HTTP_TFS_YES;

    } else {
        *((uint64_t *) p) = length;
        p += sizeof(uint64_t);
        t->has_split_frag = NGX_HTTP_TFS_NO;
    }

    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_tfs_create_del_object_message(ngx_http_tfs_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_tfs_kv_ms_del_object_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_del_object_request_t) +
        t->file.bucket_name.len + 1 +
        /* object name len */
        sizeof(uint32_t) +
        /* object name */
        t->file.object_name.len + 1 +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_del_object_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_DEL_OBJECT_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    /* object name */
    *((uint32_t *)p) = t->file.object_name.len + 1;
    p += sizeof(uint32_t);
    ngx_memcpy(p, t->file.object_name.data, t->file.object_name.len);
    p += t->file.object_name.len + 1;

    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;

    return NULL;
}


static ngx_chain_t *
ngx_http_tfs_create_head_object_message(ngx_http_tfs_t *t)
{
    u_char                                    *p;
    size_t                                     size;
    ngx_int_t                                  rc;
    ngx_buf_t                                 *b;
    ngx_chain_t                               *cl;
    ngx_http_tfs_kv_ms_head_object_request_t  *req;

    size = sizeof(ngx_http_tfs_kv_ms_head_object_request_t) +
        /* bucket name */
        t->file.bucket_name.len + 1 +
        /* object name len */
        sizeof(uint32_t) +
        /* object name */
        t->file.object_name.len + 1 +
        /* user info */
        sizeof(ngx_http_tfs_user_info_t) +
        NGX_HTTP_TFS_USER_INFO_TAG_SIZE;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_ms_head_object_request_t *) b->pos;
    req->header.type = NGX_HTTP_TFS_REQ_KVMETA_HEAD_OBJECT_MESSAGE;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.id = ngx_http_tfs_generate_packet_id();
    req->bucket_name_len = t->file.bucket_name.len + 1;
    ngx_memcpy(req->bucket_name, t->file.bucket_name.data, t->file.bucket_name.len);
    p = req->bucket_name + req->bucket_name_len;

    /* object name */
    rc = ngx_http_tfs_serialize_string(&p, &t->file.object_name);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    /* user info */
    rc = ngx_http_tfs_serialize_user_info(&p, &t->file.user_info);
    if (rc == NGX_ERROR) {
        return NULL;
    }

    b->last += size;

    req->header.len = size - sizeof(ngx_http_tfs_header_t);
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_tfs_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_int_t
ngx_http_tfs_parse_put_bucket_message(ngx_http_tfs_t *t)
{
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "put bucket (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    default:
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "put bucket response msg type is invalid: %d ", type);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_tfs_parse_get_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                    *p;
    uint16_t                                   type;
    uint32_t                                   object_meta_count, i;
    ngx_int_t                                  rc;
    ngx_str_t                                  action;
    ngx_http_tfs_header_t                     *header;
    ngx_http_tfs_peer_connection_t            *tp;
    ngx_http_tfs_object_meta_info_t           *object_meta_info;
    ngx_http_tfs_object_meta_info_chain_t   **chain, *new_chain;
    ngx_http_tfs_kv_ms_get_bucket_response_t  *resp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "get bucket (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_kv_ms_get_bucket_response_t *) tp->body_buffer.pos;

    p = tp->body_buffer.pos + sizeof(ngx_http_tfs_kv_ms_get_bucket_response_t);
    /* bucket name */
    p += resp->bucket_name_len;

    /* FIXME: all prefix/marker/common_prefix/max_keys/delimiter will be overwritten more than once */
    /* prefix */
    rc = ngx_http_tfs_deserialize_string(&p, t->pool, &t->get_bucket_ctx.prefix);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    /* marker */
    rc = ngx_http_tfs_deserialize_string(&p, t->pool, &t->get_bucket_ctx.marker);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    /* common prefix */
    rc = ngx_http_tfs_deserialize_vstring(&p, t->pool,
                                          &t->get_bucket_ctx.common_prefix_count,
                                          &t->get_bucket_ctx.common_prefix);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    /* object name */
    rc = ngx_http_tfs_deserialize_vstring(&p, t->pool,
                                          &t->get_bucket_ctx.object_name_count,
                                          &t->get_bucket_ctx.object_name);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    if (t->get_bucket_ctx.common_prefix_count == 0 &&
        t->get_bucket_ctx.object_name_count == 0)
    {
        t->file.still_have = NGX_HTTP_TFS_NO;
        return NGX_OK;
    }

    /* object meta count */
    /* FIXME: if object_meta_count != object_name_count, Das ist schlecht */
    object_meta_count = *((uint32_t *)p);
    p += sizeof(uint32_t);

    new_chain = t->get_bucket_ctx.object_meta_infos;
    chain = &t->get_bucket_ctx.object_meta_infos;
    for(; new_chain; new_chain = new_chain->next) {
        chain = &new_chain->next;
    }

    new_chain = ngx_pcalloc(t->pool, sizeof(ngx_http_tfs_object_meta_info_chain_t));
    if (new_chain == NULL) {
        return NGX_ERROR;
    }

    new_chain->meta_info = ngx_pcalloc(t->pool,
        sizeof(ngx_http_tfs_object_meta_info_t) * object_meta_count);
    if (new_chain->meta_info == NULL) {
        return NGX_ERROR;
    }
    new_chain->object_count = object_meta_count;

    *chain = new_chain;

    object_meta_info = new_chain->meta_info;

    /* object meta info */
    for (i = 0; i < object_meta_count; i++) {
        rc = ngx_http_tfs_deserialize_object_meta_info(&p, &object_meta_info[i]);
        if (rc == NGX_ERROR) {
            return NGX_ERROR;
        }
    }

    /* max keys */
    t->get_bucket_ctx.max_keys = *((uint32_t *)p);
    p += sizeof(uint32_t);

    /* is_truncated */
    t->file.still_have = *((uint8_t *)p);
    p += sizeof(uint8_t);

    /* delimiter */
    t->get_bucket_ctx.delimiter = *((uint8_t *)p);
    p += sizeof(uint8_t);

    return rc;
}


static ngx_int_t
ngx_http_tfs_parse_del_bucket_message(ngx_http_tfs_t *t)
{
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "del bucket (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    default:
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "del bucket response msg type is invalid: %d ", type);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_tfs_parse_head_bucket_message(ngx_http_tfs_t *t)
{
    u_char                                     *p;
    uint16_t                                    type;
    ngx_int_t                                   rc;
    ngx_str_t                                   action;
    ngx_http_tfs_header_t                      *header;
    ngx_http_tfs_peer_connection_t             *tp;
    ngx_http_tfs_kv_ms_head_bucket_response_t  *resp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "head bucket (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_kv_ms_head_bucket_response_t *) tp->body_buffer.pos;

    // TODO: check bucke_name?
    p = tp->body_buffer.pos + sizeof(ngx_http_tfs_kv_ms_head_bucket_response_t)
        + resp->bucket_name_len;

    rc = ngx_http_tfs_deserialize_bucket_meta_info(&p, &t->file.bucket_meta_info);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    return rc;
}


static ngx_int_t
ngx_http_tfs_parse_put_object_message(ngx_http_tfs_t *t)
{
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {

    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "put object (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    default:
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "put object response msg type is invalid: %d ", type);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_tfs_parse_get_object_message(ngx_http_tfs_t *t)
{
    u_char                                    *p;
    uint16_t                                   type;
    ngx_int_t                                  rc;
    ngx_str_t                                  action;
    ngx_http_tfs_header_t                     *header;
    ngx_http_tfs_peer_connection_t            *tp;
    ngx_http_tfs_kv_ms_get_object_response_t  *resp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "get object (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_kv_ms_get_object_response_t *) tp->body_buffer.pos;

    t->file.still_have = resp->still_have ? :t->has_split_frag;
    p = tp->body_buffer.pos + sizeof(ngx_http_tfs_kv_ms_get_object_response_t);

    t->file.object_info.tfs_file_count = 0;
    rc = ngx_http_tfs_deserialize_object_info(&p, t->pool, &t->file.object_info);

    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "still_have is %d and other is %L, %L, %L",
                                        t->file.still_have,
                  t->file.object_info.tfs_file_infos->block_id,
                  t->file.object_info.tfs_file_infos->file_id,
                  t->file.object_info.tfs_file_infos->size);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    return rc;
}


static ngx_int_t
ngx_http_tfs_parse_del_object_message(ngx_http_tfs_t *t)
{
    u_char                                    *p;
    uint16_t                                   type;
    ngx_int_t                                  rc;
    ngx_str_t                                  action;
    ngx_http_tfs_header_t                     *header;
    ngx_http_tfs_peer_connection_t            *tp;
    ngx_http_tfs_kv_ms_del_object_response_t  *resp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "del object (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_kv_ms_del_object_response_t *) tp->body_buffer.pos;

    t->file.still_have = resp->still_have;
    p = tp->body_buffer.pos + sizeof(ngx_http_tfs_kv_ms_del_object_response_t);

    /* FIXME: should make sure we can recv all data once,
      now kv meta server return 10 tfs_file_infos once */
    rc = ngx_http_tfs_deserialize_object_info(&p, t->pool, &t->file.object_info);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    return rc;
}


static ngx_int_t
ngx_http_tfs_parse_head_object_message(ngx_http_tfs_t *t)
{
    u_char                                     *p;
    uint16_t                                    type;
    uint32_t                                    object_name_len;
    ngx_int_t                                   rc;
    ngx_str_t                                   action;
    ngx_http_tfs_header_t                      *header;
    ngx_http_tfs_peer_connection_t             *tp;
    ngx_http_tfs_custom_file_info_t            *custom_file_info;
    ngx_http_tfs_kv_ms_head_object_response_t  *resp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "head object (kv meta server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_kv_ms_head_object_response_t *) tp->body_buffer.pos;

    // TODO: check bucket_name?
    p = tp->body_buffer.pos + sizeof(ngx_http_tfs_kv_ms_head_object_response_t)
        + resp->bucket_name_len;

    /* object name len */
    object_name_len = *((uint32_t *)p);
    p += sizeof(uint32_t);

    // TODO: check object_name?
    p += object_name_len;

    rc = ngx_http_tfs_deserialize_object_info(&p, t->pool,
                                              &t->file.object_info);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    /* parse to ls_file result */
    t->get_bucket_ctx.custom_file_infos = ngx_pcalloc(t->pool,
        sizeof(ngx_http_tfs_custom_file_info_chain_t));
    if (t->get_bucket_ctx.custom_file_infos == NULL) {
        return NGX_ERROR;
    }
    t->get_bucket_ctx.custom_file_infos->file_info = ngx_pcalloc(t->pool,
        sizeof(ngx_http_tfs_custom_file_info_t));
    if (t->get_bucket_ctx.custom_file_infos->file_info == NULL) {
        return NGX_ERROR;
    }
    t->get_bucket_ctx.custom_file_infos->file_count = 1;

    custom_file_info = t->get_bucket_ctx.custom_file_infos->file_info;

    custom_file_info->name = t->file.object_name;
    custom_file_info->create_time = t->file.object_info.meta_info.create_time;
    custom_file_info->modify_time = t->file.object_info.meta_info.modify_time;
    custom_file_info->size = t->file.object_info.meta_info.big_file_size;
    custom_file_info->owner_id = t->file.object_info.meta_info.owner_id;

    return rc;
}


