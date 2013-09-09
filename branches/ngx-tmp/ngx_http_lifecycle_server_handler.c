
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_http_lifecycle_errno.h>
#include <ngx_http_lifecycle_server_handler.h>
#include <ngx_http_lifecycle_kv_root_server_message.h>
#include <ngx_http_lifecycle_kv_meta_server_message.h>

/* kvroot */
ngx_int_t
ngx_http_lifecycle_create_rs_request(ngx_http_lifecycle_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_lifecycle_root_server_create_message(t->pool);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_process_rs(ngx_http_lifecycle_t *t)
{
    ngx_int_t                        rc;
    ngx_buf_t                       *b;
    ngx_http_lifecycle_inet_t             *addr;
    ngx_http_lifecycle_header_t           *header;
    ngx_http_lifecycle_peer_connection_t  *tp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    tp = t->lifecycle_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    rc = ngx_http_lifecycle_root_server_parse_message(t);
    if (rc != NGX_OK) {
        return rc;
    }
    /* meta */
    t->state += 1;

    // FIXME: tmp use
    if (rc == NGX_DONE) {
        return NGX_DONE;
    }

    addr = ngx_http_lifecycle_select_meta_server(t);
    ngx_http_lifecycle_peer_set_addr(t->pool,
        &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_META_SERVER], addr);

    return NGX_OK;
}

/* kvmeta */
ngx_int_t
ngx_http_lifecycle_create_ms_request(ngx_http_lifecycle_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_lifecycle_meta_server_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_process_ms(ngx_http_lifecycle_t *t)
{
    uint16_t                          action;
    ngx_buf_t                        *b;
    ngx_int_t                         rc;
    ngx_chain_t                      *cl, **ll;
    ngx_http_lifecycle_header_t            *header;
    ngx_http_lifecycle_peer_connection_t   *tp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    tp = t->lifecycle_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    action = t->r_ctx.action.code;
    rc = ngx_http_lifecycle_meta_server_parse_message(t);

    ngx_http_lifecycle_clear_buf(b);

    if (rc != NGX_OK) {
        /* TODO */
        return rc;
    }

    switch (action) {

    case NGX_HTTP_LIFECYCLE_ACTION_POST:
    case NGX_HTTP_LIFECYCLE_ACTION_PUT:
    case NGX_HTTP_LIFECYCLE_ACTION_DEL:
    case NGX_HTTP_LIFECYCLE_ACTION_GET:
        t->state = NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE;
        return NGX_DONE;

    default:
        return NGX_ERROR;
    }

    /* only select once */

    return rc;
}


ngx_int_t
ngx_http_lifecycle_retry_ms(ngx_http_lifecycle_t *t)
{
    ngx_http_lifecycle_inet_t             *addr;
    ngx_http_lifecycle_peer_connection_t  *tp;

    tp = t->lifecycle_peer;
    tp->peer.free(&tp->peer, tp->peer.data, 0);

    addr = ngx_http_lifecycle_select_meta_server(t);
    // TODO: return what?
    if (addr == NULL) {
        return NGX_ERROR;
    }

    ngx_http_lifecycle_peer_set_addr(t->pool,
        &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_META_SERVER], addr);

    if (ngx_http_lifecycle_reinit(t->data, t) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_http_lifecycle_connect(t);

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_process_ms_input_filter(ngx_http_lifecycle_t *t)
{
    size_t                           size;
    uint16_t                         msg_type;
    ngx_buf_t                       *b;
    ngx_http_lifecycle_header_t           *header;
    ngx_http_lifecycle_peer_connection_t  *tp;

    header = (ngx_http_lifecycle_header_t *) t->header;
    t->length = header->len;

    msg_type = header->type;
    if (msg_type == NGX_HTTP_LIFECYCLE_STATUS_MESSAGE) {
        return NGX_OK;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, t->log, 0, "get bucket rsp len is %O",
                   t->length);

    /* modify body buffer size so that it can hold all data, then we do parse */
    tp = t->lifecycle_peer;
    b = &tp->body_buffer;
    size = b->end - b->start;
    if (size < (size_t)t->length) {
        b->start = ngx_prealloc(t->pool, b->start, size, t->length);
        if (b->start == NULL) {
            return NGX_ERROR;
        }
        b->end = b->last + t->length;
    }

    b->pos = b->start;
    b->last = b->start;

    return NGX_OK;
}

