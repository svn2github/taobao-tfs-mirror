
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_http_lifecycle.h>
#include <nginx.h>
#include <ngx_http_lifecycle_errno.h>
#include <ngx_http_lifecycle_kv_meta_server_message.h>


#define ngx_http_lifecycle_clear_content_len()\
    t->header_only |= 1;                \
    r->headers_out.content_length_n = 0

ngx_str_t   ngx_lifecycle_onoff = ngx_string("on_off");
ngx_uint_t  ngx_lifecycle_onoff_index;

ngx_str_t   ngx_lifecycle_filename = ngx_string("file_name");
ngx_uint_t  ngx_lifecycle_filename_index;

ngx_str_t   ngx_lifecycle_version = ngx_string("version");
ngx_uint_t  ngx_lifecycle_version_index;

ngx_str_t   ngx_lifecycle_expiretime = ngx_string("expire_time");
ngx_uint_t  ngx_lifecycle_expiretime_index;

ngx_str_t   ngx_lifecycle_appkey = ngx_string("app_key");
ngx_uint_t  ngx_lifecycle_appkey_index;

ngx_str_t   ngx_lifecycle_action = ngx_string("action");
ngx_uint_t  ngx_lifecycle_action_index;

ngx_str_t   ngx_lifecycle_timetype = ngx_string("timetype");
ngx_uint_t  ngx_lifecycle_timetype_index;


static ngx_str_t ms_name = ngx_string("meta server");


static void ngx_http_lifecycle_event_handler(ngx_event_t *ev);


static void ngx_http_lifecycle_process_buf_overflow(ngx_http_request_t *r,
    ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_set_header_line(ngx_http_lifecycle_t *t);

static void ngx_http_lifecycle_dummy_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_read_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_send_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_send(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_send_response(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_send_filter_response(ngx_http_request_t *r, ngx_http_lifecycle_t *t);
static void ngx_http_lifecycle_process_non_buffered_downstream(ngx_http_request_t *r);
static void ngx_http_lifecycle_process_non_buffered_request(ngx_http_lifecycle_t *t, ngx_uint_t do_write);

static void ngx_http_lifecycle_process_upstream_request(ngx_http_request_t *r, ngx_http_lifecycle_t *t);

static void ngx_http_lifecycle_handle_connection_failure(ngx_http_lifecycle_t *t, ngx_http_lifecycle_peer_connection_t *tp);
static void ngx_http_lifecycle_rd_check_broken_connection(ngx_http_request_t *r);
static void ngx_http_lifecycle_wr_check_broken_connection(ngx_http_request_t *r);
static void ngx_http_lifecycle_check_broken_connection(ngx_http_request_t *r,
    ngx_event_t *ev);

extern ngx_module_t  ngx_http_lifecycle_module;


ngx_int_t
ngx_http_lifecycle_init(ngx_http_lifecycle_t *t)
{
    ngx_int_t                  rc;
    ngx_http_request_t        *r;
    ngx_http_core_loc_conf_t  *clcf;

    t->read_event_handler = ngx_http_lifecycle_read_handler;
    t->write_event_handler = ngx_http_lifecycle_send_handler;
    r = NULL;

    r = t->data;
    r->read_event_handler = ngx_http_lifecycle_rd_check_broken_connection;
    r->write_event_handler = ngx_http_lifecycle_wr_check_broken_connection;

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
    if (clcf == NULL) {
        return NGX_ERROR;
    }

    t->output.alignment = clcf->directio_alignment;
    t->output.bufs.size = clcf->client_body_buffer_size;


    t->output.pool = t->pool;
    t->output.bufs.num = 1;
    t->output.output_filter = ngx_chain_writer;
    t->output.filter_ctx = &t->writer;
    t->header_size = sizeof(ngx_http_lifecycle_header_t);
    t->writer.pool = t->pool;

    /* inited kvroot */
    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "==============will into ngx_http_lifecycle_peer_init");
    rc = ngx_http_lifecycle_peer_init(t);
    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "==============complete ngx_http_lifecycle_peer_init");
    if (rc != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "lifecycle peer init failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    /* header and body */
    t->recv_chain = ngx_http_lifecycle_alloc_chains(t->pool, 2);
    if (t->recv_chain == NULL) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "lifecycle alloc chains failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    rc = ngx_http_lifecycle_misc_ctx_init(t);
    /*
    if (rc == NGX_DECLINED) {
        if (t->decline_handler) {
            rc = t->decline_handler(t);
            if (rc == NGX_ERROR) {
                return rc;
            }
        }
        return NGX_OK;
    }

    // FIXME: tmp use
    if (rc == NGX_DONE) {
        ngx_http_lifecycle_finalize_state(t, rc);
        return NGX_OK;
    }
    */
    if (rc != NGX_OK) {
        return rc;
    }

    t->lifecycle_peer = ngx_http_lifecycle_select_peer(t);
    if (t->lifecycle_peer == NULL) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "lifecycle select peer failed");
        return NGX_ERROR;
    }

    t->recv_chain->buf = &t->header_buffer;
    t->recv_chain->next->buf = &t->lifecycle_peer->body_buffer;

    ngx_http_lifecycle_connect(t);

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_connect(ngx_http_lifecycle_t *t)
{
    ngx_int_t                        rc;
    ngx_connection_t                *c;
    ngx_http_request_t              *r;
    ngx_peer_connection_t           *p;
    ngx_http_lifecycle_peer_connection_t  *tp;

    tp = t->lifecycle_peer;
    p = &tp->peer;
    r = t->data;

    p->log->action = "connecting server";

    rc = t->create_request(t);

    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, p->log, 0, "create %V (%s) request failed",
            p->name, tp->peer_addr_text);
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return rc;
    }

    ngx_log_error(NGX_LOG_DEBUG, p->log, 0, "connecting %V, addr: %s",
                  p->name, tp->peer_addr_text);

    rc = ngx_event_connect_peer(p);

    if (rc == NGX_ERROR || rc == NGX_BUSY || rc == NGX_DECLINED) {
        ngx_log_error(NGX_LOG_ERR, p->log, 0,
                      "connect to (%V: %s) failed", p->name,
                      tp->peer_addr_text);
        ngx_http_lifecycle_handle_connection_failure(t, t->lifecycle_peer);
        return rc;
    }

    c = p->connection;
    c->data = t;

    c->read->handler = ngx_http_lifecycle_event_handler;
    c->write->handler = ngx_http_lifecycle_event_handler;

    c->sendfile &= r->connection->sendfile;
    t->output.sendfile = c->sendfile;

    if (c->pool == NULL) {
        c->pool = ngx_create_pool(128, r->connection->log);
        if (c->pool == NULL) {
            ngx_log_error(NGX_LOG_ERR, p->log, 0,
                          "create connection pool failed");
            ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return NGX_ERROR;
        }
    }

    c->log = r->connection->log;
    c->pool->log = c->log;
    c->read->log = c->log;
    c->write->log = c->log;

    t->writer.out = NULL;
    t->writer.last = &t->writer.out;
    t->writer.connection = c;
    t->writer.limit = 0;

    if (rc == NGX_AGAIN) {
        ngx_add_timer(c->write, t->main_conf->lifecycle_connect_timeout);
        return NGX_AGAIN;
    }

    ngx_http_lifecycle_send(r, t);

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_reinit(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_chain_t  *cl, *cl_next;

    t->request_sent = 0;

    for (cl = t->request_bufs; cl; cl = cl_next) {
        cl_next = cl->next;
        ngx_free_chain(r->pool, cl);
    }

    /* reinit the subrequest's ngx_output_chain() context */
    if (r->request_body && r->request_body->temp_file
        && r != r->main && t->output.buf)
    {
        t->output.free = ngx_alloc_chain_link(r->pool);
        if (t->output.free == NULL) {
            return NGX_ERROR;
        }

        t->output.free->buf = t->output.buf;
        t->output.free->next = NULL;

        t->output.buf->pos = t->output.buf->start;
        t->output.buf->last = t->output.buf->start;
    }

    t->output.buf = NULL;
    t->output.in = NULL;
    t->output.busy = NULL;

    t->header_buffer.pos = t->header_buffer.start;
    t->header_buffer.last = t->header_buffer.start;

    t->parse_state = NGX_HTTP_LIFECYCLE_HEADER;
    t->parse_body_phase = NGX_HTTP_LIFECYCLE_BODY_PHASE1;
    t->header_size = sizeof(ngx_http_lifecycle_header_t);
    t->write_event_handler = ngx_http_lifecycle_send_handler;

    return NGX_OK;
}


static void
ngx_http_lifecycle_event_handler(ngx_event_t *ev)
{
    ngx_http_lifecycle_t       *t;
    ngx_connection_t     *c;
    ngx_http_request_t   *r;
    ngx_http_log_ctx_t   *ctx;

    c = ev->data;
    t = c->data;

    r = t->data;
    c = r->connection;

    ctx = c->log->data;
    ctx->current_request = r;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http lifecycle request: \"%V?%V\"", &r->uri, &r->args);

    if (ev->write) {
        t->write_event_handler(r, t);

    } else {
        t->read_event_handler(r, t);
    }

    ngx_http_run_posted_requests(c);
}


static void
ngx_http_lifecycle_send(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_int_t                        rc;
    ngx_connection_t                *c;
    ngx_http_lifecycle_peer_connection_t  *tp;

    tp = t->lifecycle_peer;
    c = tp->peer.connection;
    /* send mes to server */
    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http lifecycle send request to %V, addr: %s", tp->peer.name,
                   tp->peer_addr_text);

    if (!t->request_sent && ngx_http_lifecycle_test_connect(c) != NGX_OK) {
        ngx_http_lifecycle_handle_connection_failure(t, tp);
        return;
    }

    c->log->action = "sending request to server";

    /* start send  */
    rc = ngx_output_chain(&t->output, t->request_sent ? NULL : t->request_bufs);

    t->request_sent = 1;

    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "ngx output chain failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    if (c->write->timer_set) {
        ngx_del_timer(c->write);
    }

    if (rc == NGX_AGAIN) {
        ngx_add_timer(c->write, t->main_conf->lifecycle_send_timeout);

        if (ngx_handle_write_event(c->write, t->main_conf->send_lowat) != NGX_OK) {
            ngx_log_error(NGX_LOG_ERR, c->log, 0,
                          "ngx handle write event failed");
            ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }

        return;
    }

    /* rc == NGX_OK */
    if (c->tcp_nopush == NGX_TCP_NOPUSH_SET) {
        if (ngx_tcp_push(c->fd) == NGX_ERROR) {
            ngx_log_error(NGX_LOG_CRIT, c->log, ngx_socket_errno,
                          ngx_tcp_push_n " failed");
            ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }

        c->tcp_nopush = NGX_TCP_NOPUSH_UNSET;
    }

    //TODO: here or there
    t->write_event_handler = ngx_http_lifecycle_dummy_handler;

    if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "ngx handle write event failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    ngx_add_timer(c->read, t->main_conf->lifecycle_read_timeout);

    // TODO: check why here always ready before data sent
    if (c->read->ready) {
        ngx_http_lifecycle_read_handler(r, t);
        return;
    }

    /*t->write_event_handler = ngx_http_lifecycle_dummy_handler;

    if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "ngx handle write event failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }*/
}


static void
ngx_http_lifecycle_dummy_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "http lifecycle dummy handler");
}


static ngx_int_t
ngx_http_lifecycle_alloc_buf(ngx_http_lifecycle_t *t)
{
    ngx_http_request_t               *r;
    ngx_http_lifecycle_peer_connection_t   *tp;

    tp = t->lifecycle_peer;
    r = t->data;

    if (t->header_buffer.start == NULL) {
        t->header_buffer.start = ngx_palloc(r->pool, t->main_conf->buffer_size);
        if (t->header_buffer.start == NULL) {
            return NGX_ERROR;
        }

        t->header_buffer.pos = t->header_buffer.start;
        t->header_buffer.last = t->header_buffer.start;
        t->header_buffer.temporary = 1;
    }

    t->header_buffer.end = t->header_buffer.start + t->header_size;

    if (tp->body_buffer.start == NULL) {
        tp->body_buffer.start = ngx_palloc(r->pool, t->main_conf->body_buffer_size);
        if (tp->body_buffer.start == NULL) {
            return NGX_ERROR;
        }

        tp->body_buffer.pos = tp->body_buffer.start;
        tp->body_buffer.last = tp->body_buffer.start;
        tp->body_buffer.end = tp->body_buffer.start + t->main_conf->body_buffer_size;
        tp->body_buffer.temporary = 1;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_process_header(ngx_http_lifecycle_t *t, ngx_int_t n)
{
    ngx_int_t        body_size, rc;

    if (n < t->header_size) {
        t->header_buffer.last += n;
        t->header_size -= n;
        return NGX_AGAIN;
    }

    t->header_buffer.last += t->header_size;
    t->header = (void *) t->header_buffer.pos;

    body_size = n - t->header_size;
    if (t->input_filter != NULL) {
        rc = t->input_filter(t);
        if (rc != NGX_OK) { /* error or NGX_DONE */
            return rc;
        }
    }

    if (body_size > 0) {
        return body_size;
    }

    return NGX_DECLINED;
}


static void
ngx_http_lifecycle_send_filter_response(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{

    if (!r->header_sent) {
        ngx_http_lifecycle_set_header_line(t);
    }
    ngx_http_lifecycle_send_body(t);

}


void
ngx_http_lifecycle_finalize_state(ngx_http_lifecycle_t *t, ngx_int_t rc)
{
    uint16_t                          action;
    ngx_http_request_t               *r;
    ngx_peer_connection_t            *p;
    ngx_http_lifecycle_peer_connection_t   *tp;

    r = t->data;
    tp = t->lifecycle_peer;
    p = NULL;

    if (tp) {
        p = &tp->peer;
        if (p) {
            ngx_log_error(NGX_LOG_INFO, t->log, 0,
                          "http lifecycle finalize state %V, %i", p->name, rc);
        }
    }


    if (rc == NGX_HTTP_CLIENT_CLOSED_REQUEST
        || rc == NGX_HTTP_REQUEST_TIME_OUT)
    {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "client prematurely closed connection or timed out");
        ngx_http_lifecycle_finalize_request(r, t, rc);
        return;
    }

    if (rc == NGX_ERROR) {
        if (p) {
            ngx_log_error(NGX_LOG_ERR, t->log, 0,
                          "http lifecycle process %V request failed", p->name);
        }

        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }
    /* return other error */
    if (rc >= NGX_HTTP_SPECIAL_RESPONSE
        || rc <= NGX_HTTP_LIFECYCLE_EXIT_GENERAL_ERROR)
    {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "========http lifecycle process request failed=========");
        t->lifecycle_status = rc;
        ngx_http_lifecycle_send_filter_response(r, t);
        return;
    }


    if (rc == NGX_HTTP_LIFECYCLE_AGAIN) {
        if (t->retry_handler) {
            rc = t->retry_handler(t);
            if (rc == NGX_OK || rc == NGX_DECLINED) {
                return;
            }

        }

        t->lifecycle_status = NGX_ERROR;
        ngx_http_lifecycle_send_response(r, t);

        return;
    }

    if (rc == NGX_DONE) {
        /* need send data */
        //ngx_http_lifecycle_send_response(r, t);

        action = t->r_ctx.action.code;
        switch (action) {

        case NGX_HTTP_LIFECYCLE_ACTION_POST:
            ngx_http_lifecycle_send_filter_response(r, t);
            break;
        case NGX_HTTP_LIFECYCLE_ACTION_PUT:
            ngx_http_lifecycle_send_filter_response(r, t);
            break;
        case NGX_HTTP_LIFECYCLE_ACTION_DEL:
            break;
        case NGX_HTTP_LIFECYCLE_ACTION_GET:
            ngx_http_lifecycle_send_filter_response(r, t);
            return;

        default:
            return;
        }
        return;
    }

    if (p && p->free) {
        p->free(p, p->data, 0);
    }

    if (rc == NGX_DECLINED) {
        if (t->decline_handler) {
            rc = t->decline_handler(t);
            if (rc == NGX_ERROR) {
                ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
            }
        }
        return;
    }

    /* rc == NGX_OK */
    if (ngx_http_lifecycle_reinit(r, t) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "lifecycle reinit failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    t->lifecycle_peer = ngx_http_lifecycle_select_peer(t);
    if (t->lifecycle_peer == NULL) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "lifecycle select peer failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    t->recv_chain->buf = &t->header_buffer;
    t->recv_chain->next->buf = &t->lifecycle_peer->body_buffer;

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "http lifecycle process next peer is %V, addr: %s", t->lifecycle_peer->peer.name,
                  t->lifecycle_peer->peer_addr_text);

    ngx_http_lifecycle_connect(t);
}


static void
ngx_http_lifecycle_process_upstream_request(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_int_t                         n, rc;
    ngx_chain_t                      *chain;
    ngx_connection_t                 *c;
    ngx_peer_connection_t            *p;
    ngx_http_lifecycle_peer_connection_t   *tp;

    tp = t->lifecycle_peer;
    p = &tp->peer;
    c = p->connection;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http lifecycle process request body for %V, addr: %s", p->name,
                   tp->peer_addr_text);

    if (!t->request_sent && ngx_http_lifecycle_test_connect(c) != NGX_OK) {
        ngx_http_lifecycle_handle_connection_failure(t, tp);
        return;
    }

    rc = ngx_http_lifecycle_alloc_buf(t);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "lifecycle alloc buf failed");
        ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    if (c->read->timer_set) {
        ngx_del_timer(c->read);
    }

    for ( ;; ) {

        for (chain = t->recv_chain; chain; chain = chain->next) {
            if (chain->buf->last != chain->buf->end) {
                break;
            }
        }

        if (chain == NULL) {
            /* need process null  data */
            ngx_http_lifecycle_process_buf_overflow(r, t);
            return;
        }

        n = c->recv_chain(c, chain);

        if (n == NGX_AGAIN) {
            if (chain->buf->last == chain->buf->end) {
                ngx_http_lifecycle_process_buf_overflow(r, t);
                return;
            }

            ngx_add_timer(c->read, t->main_conf->lifecycle_read_timeout);
            if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
                ngx_log_error(NGX_LOG_ERR, c->log, 0,
                              "lifecycle handle read event failed");
                ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
                return;
            }

            return;
        }

        if (n == 0) {
            ngx_log_error(NGX_LOG_ERR, c->log, 0,
                          "lifecycle prematurely closed connection");
        }

        if (n == NGX_ERROR || n == 0) {
            ngx_log_error(NGX_LOG_ERR, c->log, 0,
                          "recv chain error");
            if (ngx_strncmp(p->name->data, ms_name.data, p->name->len) == 0) {
                ngx_http_lifecycle_finalize_state(t, NGX_HTTP_LIFECYCLE_AGAIN);
                return;
            }
            ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }

        if (t->parse_state == NGX_HTTP_LIFECYCLE_HEADER) {
            rc = ngx_http_lifecycle_process_header(t, n);

            if (rc == NGX_DECLINED) {
                t->parse_state = NGX_HTTP_LIFECYCLE_BODY;
            }

            if (rc == NGX_AGAIN || rc == NGX_DECLINED) {
                continue;
            }

            if (rc < 0 || rc == NGX_DONE) {
                break;
            }

            t->parse_state = NGX_HTTP_LIFECYCLE_BODY;
            n = rc;
        }

        tp->body_buffer.last += n;

        rc = t->process_request_body(t);

        if (rc == NGX_AGAIN) {
            continue;
        }

        break;
    }

    /* rc == NGX_OK */
    ngx_http_lifecycle_finalize_state(t, rc);
}


static void
ngx_http_lifecycle_send_response(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{

    int                           tcp_nodelay;
    ngx_int_t                     rc;
    ngx_connection_t             *c;
    ngx_http_core_loc_conf_t     *clcf;


    if (!r->header_sent) {
        ngx_http_lifecycle_set_header_line(t);

        rc = ngx_http_send_header(r);


        if (rc == NGX_ERROR || rc > NGX_OK || r->post_action) {
            ngx_http_lifecycle_finalize_state(t, rc);
            return;
        }

        if (t->header_only) {
            ngx_http_lifecycle_finalize_request(r, t, rc);
            return;
        }
    }
    /*
    c = r->connection;

    if (r->request_body && r->request_body->temp_file) {
        ngx_pool_run_cleanup_file(r->pool, r->request_body->temp_file->file.fd);
        r->request_body->temp_file->file.fd = NGX_INVALID_FILE;
    }

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

    r->write_event_handler = ngx_http_lifecycle_process_non_buffered_downstream;

    r->limit_rate = 0;

    if (clcf->tcp_nodelay && c->tcp_nodelay == NGX_TCP_NODELAY_UNSET) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "tcp_nodelay");

        tcp_nodelay = 1;

        if (setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY,
                (const void *) &tcp_nodelay, sizeof(int)) == -1)
        {
            ngx_connection_error(c, ngx_socket_errno,
                                 "setsockopt(TCP_NODELAY) failed");

            ngx_http_lifecycle_finalize_request(r, t, 0);
            return;
        }

        c->tcp_nodelay = NGX_TCP_NODELAY_SET;
    }

    ngx_http_lifecycle_process_non_buffered_downstream(r);
    */
    return;
}


static void
ngx_http_lifecycle_set_header_line(ngx_http_lifecycle_t *t)
{
    /* error code into http */
    ngx_http_request_t          *r;
    ngx_http_lifecycle_restful_ctx_t  *ctx;

    r = t->data;
    ctx = &t->r_ctx;

    /* common error */
    switch (t->lifecycle_status) {
    case NGX_ERROR:
    case NGX_HTTP_LIFECYCLE_EXIT_GENERAL_ERROR:
        r->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR;
        goto error_header;
    case NGX_HTTP_SPECIAL_RESPONSE ... NGX_HTTP_INTERNAL_SERVER_ERROR:
        r->headers_out.status = t->lifecycle_status;
        goto error_header;
    case NGX_HTTP_LIFECYCLE_EXIT_INVALID_FILE_NAME:
    case NGX_HTTP_LIFECYCLE_EXIT_DISK_OPER_INCOMPLETE:
    case NGX_HTTP_LIFECYCLE_EXIT_INVALID_ARGU_ERROR:
        r->headers_out.status = NGX_HTTP_BAD_REQUEST;
        goto error_header;
    case NGX_HTTP_LIFECYCLE_EXIT_FILE_INFO_ERROR:
    case NGX_HTTP_LIFECYCLE_EXIT_FILE_STATUS_ERROR:
        r->headers_out.status = NGX_HTTP_NOT_FOUND;
        goto error_header;
    case NGX_HTTP_LIFECYCLE_EXIT_VERSION_CONFLICT_ERROR:
        r->headers_out.status = NGX_HTTP_CONFLICT;
        goto error_header;
    }

    switch(ctx->action.code) {

    case NGX_HTTP_LIFECYCLE_ACTION_GET:
        switch (t->state) {
          /*TODO INSERT EXPIRE TIME */
        case NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE:
            r->headers_out.content_type_len = sizeof("application/json") - 1;
            ngx_str_set(&r->headers_out.content_type, "application/json");
            r->headers_out.status = NGX_HTTP_OK;
            break;
            /* errno */
        default:
            switch (t->lifecycle_status) {
            default:
                r->headers_out.status = NGX_HTTP_OK;
                //r->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR;
                break;
            }
            //goto error_header;
        }
        break;


    case NGX_HTTP_LIFECYCLE_ACTION_POST:
    case NGX_HTTP_LIFECYCLE_ACTION_PUT:
        switch (t->state) {
        case NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE:
            r->headers_out.content_type_len = sizeof("application/json") - 1;
            ngx_str_set(&r->headers_out.content_type, "application/json");
            r->headers_out.status = NGX_HTTP_CREATED;
            break;
            /* errno */
        default:
            switch (t->lifecycle_status) {
            default:
                r->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR;
                break;
            }
            goto error_header;
        }
        break;

    case NGX_HTTP_LIFECYCLE_ACTION_DEL:
        switch (t->state) {
        case NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE:
            ngx_http_lifecycle_clear_content_len();
            r->headers_out.status = NGX_HTTP_OK;
            break;
            /* errno */
        default:
            switch (t->lifecycle_status) {
            default:
                r->headers_out.status = NGX_HTTP_INTERNAL_SERVER_ERROR;
                break;
            }
            goto error_header;
        }
        break;

    default:
        break;
    }

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "%V success", &ctx->action.msg);
    return;

error_header:
    ngx_http_lifecycle_clear_content_len();

    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                  "%V failed, err(%d)",
                  &ctx->action.msg, t->lifecycle_status);
}

/*
static void
ngx_http_lifecycle_process_non_buffered_downstream(ngx_http_request_t *r)
{
    ngx_event_t          *wev;
    ngx_http_lifecycle_t       *t;
    ngx_connection_t     *c;

    c = r->connection;
    wev = c->write;
    t = ngx_http_get_module_ctx(r, ngx_http_lifecycle_module);

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http lifecycle upstream process downstream");

    c->log->action = "sending to client";

    if (wev->timedout) {
        c->timedout = 1;
        ngx_connection_error(c, NGX_ETIMEDOUT, "client timed out");

        ngx_http_lifecycle_finalize_request(t->data, t, NGX_HTTP_REQUEST_TIME_OUT);
        return;
    }

    ngx_http_lifecycle_process_non_buffered_request(t, 1);
}


static void
ngx_http_lifecycle_process_non_buffered_request(ngx_http_lifecycle_t *t, ngx_uint_t do_write)
{
    size_t                     size;
    ssize_t                    n;
    ngx_int_t                  rc, finalize_state;
    ngx_buf_t                 *b;
    ngx_connection_t          *downstream, *upstream;
    ngx_http_request_t        *r;
    ngx_http_core_loc_conf_t  *clcf;

    r = t->data;
    finalize_state = 0;
    rc = 0;
    b = NULL;
    downstream = r->connection;
    upstream = NULL;

                ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                    "=========into ngx_http_output_filter");

    b = &t->lifecycle_peer->body_buffer;
    upstream = t->lifecycle_peer->peer.connection;

    for ( ;; ) {
        if (do_write) {

            if (t->out_bufs || t->busy_bufs) {
                rc = ngx_http_output_filter(r, t->out_bufs);

                if (rc == NGX_ERROR) {
                    ngx_http_lifecycle_finalize_request(r, t, 0);
                    return;
                }

#if defined(nginx_version) && (nginx_version > 1001003)
                ngx_chain_update_chains(t->pool, &t->free_bufs, &t->busy_bufs,
                                        &t->out_bufs, t->output.tag);
#else
                ngx_chain_update_chains(&t->free_bufs, &t->busy_bufs,
                                        &t->out_bufs, t->output.tag);
#endif
            }

            // send all end

            if (t->busy_bufs == NULL) {

                t->output_size += t->main_conf->body_buffer_size;

                if (t->state == NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE)
                {
                    // need log size
                    ngx_log_error(NGX_LOG_INFO, t->log, 0, "%V, output %uL byte",
                                  &t->r_ctx.action.msg, t->output_size);
                    ngx_http_lifecycle_finalize_request(r, t, 0);
                    return;
                }

                ngx_http_lifecycle_clear_buf(b);
            }
        }

        size = b->end - b->last;

        if (t->length > 0 && size) {
            if (upstream == NULL) {
                ngx_log_error(NGX_LOG_ERR, t->log, 0, "upstream is NULL!");
                ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
                return;
            }

            if (upstream->read->ready) {

                n = upstream->recv(upstream, b->last, size);

                if (n == NGX_AGAIN) {
                    break;
                }

                if (n > 0) {
                    b->last += n;
                    do_write = 1;

                    // copy buf to out_bufs
                    rc = t->process_request_body(t);
                    if (rc == NGX_ERROR) {
                        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                      "process request body failed");
                        ngx_http_lifecycle_finalize_request(t->data, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
                        return;
                    }

                    // ngx_ok or ngx_done
                    if (rc != NGX_AGAIN) {
                        finalize_state = 1;
                        break;
                    }
                }

                continue;
            }
        }

        break;
    }

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

    if (downstream->data == r) {
        if (ngx_handle_write_event(downstream->write, clcf->send_lowat)
            != NGX_OK)
        {
            ngx_http_lifecycle_finalize_request(r, t, 0);
            return;
        }
    }

    if (downstream->write->active && !downstream->write->ready) {
        ngx_add_timer(downstream->write, clcf->send_timeout);

    } else if (downstream->write->timer_set) {
        ngx_del_timer(downstream->write);
    }

    if (upstream) {
        if (t->length > 0 && upstream->read->active && !upstream->read->ready) {
            ngx_add_timer(upstream->read, t->main_conf->lifecycle_read_timeout);

        } else if (upstream->read->timer_set) {
            ngx_del_timer(upstream->read);
        }
    }

    if (finalize_state) {
        ngx_http_lifecycle_finalize_state(t, rc);
    }
}
*/

static void
ngx_http_lifecycle_process_buf_overflow(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_int_t                rc;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                  "lifecycle process buf overflow, %V", t->lifecycle_peer->peer.name);

    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                  "action: %V should not come to here process buf overflow", &t->r_ctx.action.msg);

    ngx_http_lifecycle_finalize_request(r, t, NGX_HTTP_INTERNAL_SERVER_ERROR);
}


void
ngx_http_lifecycle_finalize_request(ngx_http_request_t *r, ngx_http_lifecycle_t *t, ngx_int_t rc)
{
    ngx_uint_t                     i;
    ngx_http_lifecycle_t                *next_st;
    ngx_peer_connection_t         *p;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "finalize http lifecycle request: %i", rc);

    for (i = 0; i < t->lifecycle_peer_count; i++) {
        p = &t->lifecycle_peer_servers[i].peer;
        if (p->free) {
            p->free(p, p->data, 0);
        }

        if (p->connection) {
            if (p->free) {
                p->free(p, p->data, 0);
            }

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                           "close http upstream connection: %d",
                           p->connection->fd);

            if (p->connection->pool) {
                ngx_destroy_pool(p->connection->pool);
            }

            ngx_close_connection(p->connection);
        }

        p->connection = NULL;
    }
#if (NGX_DEBUG)
    ngx_http_lifecycle_connection_pool_check(t->main_conf->conn_pool, t->log);
#endif

    if (t->json_output) {
        ngx_http_lifecycle_json_destroy(t->json_output);
    }


    r->connection->log->action = "sending to client";
    if (rc == NGX_OK) {
        rc = ngx_http_send_special(r, NGX_HTTP_LAST);
    }

    ngx_http_finalize_request(r, rc);
}


static void
ngx_http_lifecycle_read_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_connection_t                 *c;
    ngx_http_lifecycle_peer_connection_t   *tp;

    tp = t->lifecycle_peer;
    c = tp->peer.connection;
    /* recive data from server */
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http lifecycle process lifecycle(%V) data", tp->peer.name);

    c->log->action = "reading response header from lifecycle";

    if (c->read->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "read from (%V: %s) timeout", tp->peer.name,
                      tp->peer_addr_text);
        ngx_http_lifecycle_handle_connection_failure(t, tp);
        return;
    }

    ngx_http_lifecycle_process_upstream_request(r, t);
}


static void
ngx_http_lifecycle_send_handler(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_connection_t                 *c;
    ngx_http_lifecycle_peer_connection_t   *tp;

    tp = t->lifecycle_peer;
    c = tp->peer.connection;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "http lifecycle send request");

    c->log->action = "sending request to lifecycle";

    if (c->write->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                      "connect or send to (%V: %s) timeout", tp->peer.name,
                      tp->peer_addr_text);
        ngx_http_lifecycle_handle_connection_failure(t, tp);
        return;
    }

    ngx_http_lifecycle_send(r, t);
}


static void
ngx_http_lifecycle_handle_connection_failure(ngx_http_lifecycle_t *t, ngx_http_lifecycle_peer_connection_t *tp)
{
    char                            *ip;
    uint16_t                         port;
    ngx_connection_t                *c;
    ngx_peer_connection_t           *p;
#if (NGX_DEBUG)
    ngx_http_lifecycle_connection_pool_t      *pool;
#endif
    ngx_http_lifecycle_peer_connection_t  *root_server;

    p = &tp->peer;
    c = p->connection;
#if (NGX_DEBUG)
    /* failure connection can not be freed,
     so make sure get&free op pairs are right */
    pool = p->data;
    pool->count++;
#endif

    /* close failure connection */
    if (c != NULL) {
        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, t->log, 0,
                       "close http upstream connection: %d",
                       c->fd);

        if (c->pool) {
            ngx_destroy_pool(c->pool);
        }

        ngx_close_connection(c);
    }
    p->connection = NULL;

    /* connect metaserver fail, get new table from rootserver */
    if (ngx_strncmp(p->name->data, ms_name.data, p->name->len) == 0
        && t->loc_conf->meta_fail_count++ >= t->loc_conf->update_kmt_fail_count)
    {
        t->state = NGX_HTTP_LIFECYCLE_STATE_ACTION_GET_META_TABLE;
        root_server = &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_ROOT_SERVER];
        if (t->main_conf->krs_addr != NULL) {
            root_server->peer.sockaddr = t->main_conf->krs_addr->sockaddr;
            root_server->peer.socklen = t->main_conf->krs_addr->socklen;
            ip = inet_ntoa(((struct sockaddr_in*)
                           (root_server->peer.sockaddr))->sin_addr);
            port = ntohs(((struct sockaddr_in*)
                          (root_server->peer.sockaddr))->sin_port);
            ngx_sprintf(root_server->peer_addr_text, "%s:%d", ip, port);

        } else {
            ngx_http_lifecycle_peer_set_addr(t->pool, root_server,
                (ngx_http_lifecycle_inet_t *)&t->loc_conf->meta_root_server);
        }

        ngx_http_lifecycle_finalize_state(t, NGX_OK);
        return;
    }

    ngx_http_lifecycle_finalize_state(t, NGX_HTTP_LIFECYCLE_AGAIN);
}

/*
ngx_int_t
ngx_http_lifecycle_set_output_appid(ngx_http_lifecycle_t *t, uint64_t app_id)
{
    ngx_chain_t                 *cl, **ll;

    t->json_output = ngx_http_lifecycle_json_init(t->log, t->pool);
    if (t->json_output == NULL) {
        return NGX_ERROR;
    }

    for (cl = t->out_bufs, ll = &t->out_bufs; cl; cl = cl->next) {
        ll = &cl->next;
    }

    cl = ngx_http_lifecycle_json_appid(t->json_output, app_id);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    *ll = cl;
    return NGX_OK;
}

*/
ngx_int_t
ngx_http_lifecycle_misc_ctx_init(ngx_http_lifecycle_t *t)
{
    char                             *ip;
    uint16_t                          port;
    ngx_http_request_t               *r;
    ngx_http_lifecycle_inet_t              *addr;


    if (t->main_conf->krs_addr == NULL) {
        /* check kv root server */
        return NGX_HTTP_BAD_REQUEST;
    }

    /* add kv meta access count */
    if (++t->loc_conf->meta_access_count >= t->loc_conf->update_kmt_interval_count
        || t->loc_conf->meta_fail_count >= t->loc_conf->update_kmt_fail_count)
    {
        t->loc_conf->meta_server_table.valid = NGX_HTTP_LIFECYCLE_NO;
    }

    /* check meta table no need update */
    if (t->loc_conf->meta_server_table.valid) {
        /* direct to NGX_HTTP_LIFECYCLE_STATE_ACTION_PROCESS */
        t->state += 1;

        addr = ngx_http_lifecycle_select_meta_server(t);

        ngx_http_lifecycle_peer_set_addr(t->pool,
            &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_META_SERVER], addr);

    }

    return NGX_OK;
}


static void
ngx_http_lifecycle_rd_check_broken_connection(ngx_http_request_t *r)
{
    ngx_http_lifecycle_check_broken_connection(r, r->connection->read);
}


static void
ngx_http_lifecycle_wr_check_broken_connection(ngx_http_request_t *r)
{
    ngx_http_lifecycle_check_broken_connection(r, r->connection->write);
}


static void
ngx_http_lifecycle_check_broken_connection(ngx_http_request_t *r,
    ngx_event_t *ev)
{
    int               n;
    char              buf[1];
    ngx_err_t         err;
    ngx_int_t         event;
    ngx_http_lifecycle_t    *t;
    ngx_connection_t  *c;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, ev->log, 0,
                   "http lifecycle check client, write event:%d, \"%V\"",
                   ev->write, &r->uri);

    c = r->connection;
    t = ngx_http_get_module_ctx(r, ngx_http_lifecycle_module);

    if (c->error) {
        if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && ev->active) {

            event = ev->write ? NGX_WRITE_EVENT : NGX_READ_EVENT;

            ngx_del_event(ev, event, 0);
        }

        t->client_abort = NGX_HTTP_LIFECYCLE_YES;

        return;
    }

#if (NGX_HAVE_KQUEUE)

    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT) {

        if (!ev->pending_eof) {
            return;
        }

        ev->eof = 1;
        c->error = 1;

        if (ev->kq_errno) {
            ev->error = 1;
        }

        ngx_log_error(NGX_LOG_INFO, ev->log, ev->kq_errno,
                      "kevent() reported that client prematurely closed "
                      "connection");
        t->client_abort = NGX_HTTP_LIFECYCLE_YES;

        return;
    }

#endif

    n = recv(c->fd, buf, 1, MSG_PEEK);

    err = ngx_socket_errno;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, ev->log, err,
                   "http lifecycle recv(): %d", n);

    if (ev->write && (n >= 0 || err == NGX_EAGAIN)) {
        return;
    }

    if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && ev->active) {

        event = ev->write ? NGX_WRITE_EVENT : NGX_READ_EVENT;

        ngx_del_event(ev, event, 0);
    }

    if (n > 0) {
        return;
    }

    if (n == -1) {
        if (err == NGX_EAGAIN) {
            return;
        }

        ev->error = 1;

    } else { /* n == 0 */
        err = 0;
    }

    ev->eof = 1;
    c->error = 1;

    ngx_log_error(NGX_LOG_INFO, ev->log, err,
                  "client prematurely closed connection");

    t->client_abort = NGX_HTTP_LIFECYCLE_YES;
}
