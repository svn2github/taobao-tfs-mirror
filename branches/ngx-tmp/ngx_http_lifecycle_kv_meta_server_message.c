
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_http_lifecycle_serialization.h>
#include <ngx_http_lifecycle_kv_meta_server_message.h>
#include <ngx_http_lifecycle_protocol.h>
#include <ngx_http_lifecycle_errno.h>


static ngx_chain_t *ngx_http_lifecycle_create_set_lifecycle_message(ngx_http_lifecycle_t *t);
static ngx_chain_t *ngx_http_lifecycle_create_get_lifecycle_message(ngx_http_lifecycle_t *t);
static ngx_chain_t *ngx_http_lifecycle_create_del_lifecycle_message(ngx_http_lifecycle_t *t);

static ngx_int_t ngx_http_lifecycle_parse_set_lifecycle_message(ngx_http_lifecycle_t *t);
static ngx_int_t ngx_http_lifecycle_parse_get_lifecycle_message(ngx_http_lifecycle_t *t);
static ngx_int_t ngx_http_lifecycle_parse_del_lifecycle_message(ngx_http_lifecycle_t *t);

ngx_http_lifecycle_inet_t *
ngx_http_lifecycle_select_meta_server(ngx_http_lifecycle_t *t)
{
    uint32_t  index;

    if (t->kv_meta_retry++ > NGX_HTTP_LIFECYCLE_KV_META_RETRY_COUNT) {
        return NULL;
    }

    index = ngx_random() % t->loc_conf->meta_server_table.size;

    return &(t->loc_conf->meta_server_table.table[index]);
}


ngx_chain_t *
ngx_http_lifecycle_meta_server_create_message(ngx_http_lifecycle_t *t)
{
    uint16_t      action;
    ngx_chain_t  *cl;

    cl = NULL;
    action = t->r_ctx.action.code;

    switch (action) {

    case NGX_HTTP_LIFECYCLE_ACTION_POST:
    case NGX_HTTP_LIFECYCLE_ACTION_PUT:
        cl = ngx_http_lifecycle_create_set_lifecycle_message(t);
        break;

    case NGX_HTTP_LIFECYCLE_ACTION_DEL:
        cl = ngx_http_lifecycle_create_del_lifecycle_message(t);
        break;

    case NGX_HTTP_LIFECYCLE_ACTION_GET:
        cl = ngx_http_lifecycle_create_get_lifecycle_message(t);
        break;
    }

    return cl;
}


ngx_int_t
ngx_http_lifecycle_meta_server_parse_message(ngx_http_lifecycle_t *t)
{
    uint16_t  action;

    action = t->r_ctx.action.code;

    switch (action) {

    case NGX_HTTP_LIFECYCLE_ACTION_POST:
    case NGX_HTTP_LIFECYCLE_ACTION_PUT:
        return ngx_http_lifecycle_parse_set_lifecycle_message(t);

    case NGX_HTTP_LIFECYCLE_ACTION_GET:
        return ngx_http_lifecycle_parse_get_lifecycle_message(t);

    case NGX_HTTP_LIFECYCLE_ACTION_DEL:
        return ngx_http_lifecycle_parse_del_lifecycle_message(t);


    default:
        return NGX_ERROR;
    }

    return NGX_ERROR;
}


static ngx_chain_t *
ngx_http_lifecycle_create_set_lifecycle_message(ngx_http_lifecycle_t *t)
{
    u_char                                   *p;
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_kv_ms_set_lifecycle_request_t   *req;

    size = sizeof(ngx_http_kv_ms_set_lifecycle_request_t) +

        t->r_ctx.file_name.len + 1 +
        /* invalid_time_s_ */
        sizeof(int32_t) +
        /* appkey len */
        sizeof(uint32_t) +
        /* appkey */
        t->r_ctx.appkey.len + 1;


    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_kv_ms_set_lifecycle_request_t *) b->pos;
    req->header.type = NGX_HTTP_LIFECYCLE_REQ_KV_SET_LIFE_CYCLE_MESSAGE;
    req->header.flag = NGX_HTTP_LIFECYCLE_PACKET_FLAG;
    req->header.version = NGX_HTTP_LIFECYCLE_PACKET_VERSION;
    req->header.id = ngx_http_lifecycle_generate_packet_id();
    req->file_type = (int32_t)t->r_ctx.version;
    req->file_name_len = t->r_ctx.file_name.len + 1;
    ngx_memcpy(req->file_name, t->r_ctx.file_name.data, t->r_ctx.file_name.len);
    p = req->file_name + req->file_name_len;

    *((int32_t *)p) = t->r_ctx.absolute_time;
    p += sizeof(int32_t);

    *((uint32_t *)p) = t->r_ctx.appkey.len + 1;
    p += sizeof(uint32_t);

    ngx_memcpy(p, t->r_ctx.appkey.data, t->r_ctx.appkey.len);
    b->last += size;

    req->header.len = size - sizeof(ngx_http_lifecycle_header_t);
    req->header.crc = ngx_http_lifecycle_crc(NGX_HTTP_LIFECYCLE_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_lifecycle_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_lifecycle_create_del_lifecycle_message(ngx_http_lifecycle_t *t)
{
    size_t                                    size;
    ngx_int_t                                 rc;
    ngx_buf_t                                *b;
    ngx_chain_t                              *cl;
    ngx_http_kv_ms_del_lifecycle_request_t       *req;

    size = sizeof(ngx_http_kv_ms_del_lifecycle_request_t) +
        /* TODO */
        t->r_ctx.file_name.len + 1;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_kv_ms_del_lifecycle_request_t *) b->pos;
    req->header.type = NGX_HTTP_LIFECYCLE_REQ_KV_DEL_LIFE_CYCLE_MESSAGE;
    req->header.flag = NGX_HTTP_LIFECYCLE_PACKET_FLAG;
    req->header.version = NGX_HTTP_LIFECYCLE_PACKET_VERSION;
    req->header.id = ngx_http_lifecycle_generate_packet_id();
    req->file_type = (int32_t)t->r_ctx.version;
    req->file_name_len = t->r_ctx.file_name.len + 1;
    ngx_memcpy(req->file_name, t->r_ctx.file_name.data, t->r_ctx.file_name.len);

    b->last += size;

    req->header.len = size - sizeof(ngx_http_lifecycle_header_t);
    req->header.crc = ngx_http_lifecycle_crc(NGX_HTTP_LIFECYCLE_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_lifecycle_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_chain_t *
ngx_http_lifecycle_create_get_lifecycle_message(ngx_http_lifecycle_t *t)
{
    size_t                                        size;
    ngx_int_t                                     rc;
    ngx_buf_t                                    *b;
    ngx_chain_t                                  *cl;
    ngx_http_kv_ms_get_lifecycle_request_t       *req;

    size = sizeof(ngx_http_kv_ms_get_lifecycle_request_t) +
        /* TODO */
        t->r_ctx.file_name.len + 1;

    b = ngx_create_temp_buf(t->pool, size);
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_kv_ms_get_lifecycle_request_t *) b->pos;
    req->header.type = NGX_HTTP_LIFECYCLE_REQ_KV_GET_LIFE_CYCLE_MESSAGE;
    req->header.flag = NGX_HTTP_LIFECYCLE_PACKET_FLAG;
    req->header.version = NGX_HTTP_LIFECYCLE_PACKET_VERSION;
    req->header.id = ngx_http_lifecycle_generate_packet_id();
    req->file_type = (int32_t)t->r_ctx.version;
    req->file_name_len = t->r_ctx.file_name.len + 1;
    ngx_memcpy(req->file_name, t->r_ctx.file_name.data, t->r_ctx.file_name.len);

    b->last += size;

    req->header.len = size - sizeof(ngx_http_lifecycle_header_t);
    req->header.crc = ngx_http_lifecycle_crc(NGX_HTTP_LIFECYCLE_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       size - sizeof(ngx_http_lifecycle_header_t));

    cl = ngx_alloc_chain_link(t->pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


static ngx_int_t
ngx_http_lifecycle_parse_set_lifecycle_message(ngx_http_lifecycle_t *t)
{
    uint16_t                               type;
    ngx_str_t                              action;
    ngx_http_lifecycle_header_t           *header;
    ngx_http_lifecycle_peer_connection_t  *tp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    tp = t->lifecycle_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_LIFECYCLE_STATUS_MESSAGE:
        ngx_str_set(&action, "set lifecycle (kv meta server)");
        return ngx_http_lifecycle_status_message(&tp->body_buffer, &action, t->log);
    default:
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "set lifecycle response msg type is invalid: %d ", type);
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_lifecycle_parse_del_lifecycle_message(ngx_http_lifecycle_t *t)
{
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_http_lifecycle_header_t           *header;
    ngx_http_lifecycle_peer_connection_t  *tp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    tp = t->lifecycle_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_LIFECYCLE_STATUS_MESSAGE:
        ngx_str_set(&action, "del lifecycle (kv meta server)");
        return ngx_http_lifecycle_status_message(&tp->body_buffer, &action, t->log);
    default:
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "del lifecycle response msg type is invalid: %d ", type);
        return NGX_ERROR;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_parse_get_lifecycle_message(ngx_http_lifecycle_t *t)
{
    u_char                                             *p;
    uint16_t                                            type;
    ngx_str_t                                           action;
    ngx_http_lifecycle_header_t                        *header;
    ngx_http_lifecycle_peer_connection_t               *tp;
    ngx_http_kv_ms_get_lifecycle_response_t            *resp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    tp = t->lifecycle_peer;
    type = header->type;

    switch (type) {
    case NGX_HTTP_LIFECYCLE_STATUS_MESSAGE:
        ngx_str_set(&action, "get lifecycle (kv meta server)");
        return ngx_http_lifecycle_status_message(&tp->body_buffer, &action, t->log);
    case NGX_HTTP_LIFECYCLE_RSP_KV_GET_LIFE_CYCLE_MESSAGE:
        ngx_str_set(&action, "get lifecycle (kv meta server)");
        resp = (ngx_http_kv_ms_get_lifecycle_response_t *) tp->body_buffer.pos;
        t->r_ctx.absolute_time = resp->invalid_time_s;
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "t->r_ctx.absolute_time is: %d ", t->r_ctx.absolute_time);
        return NGX_OK;

    }

    return NGX_OK;
}

