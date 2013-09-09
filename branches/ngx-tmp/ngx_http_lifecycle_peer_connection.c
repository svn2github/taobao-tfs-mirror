
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#include <ngx_http_lifecycle_peer_connection.h>
#include <ngx_http_lifecycle_server_handler.h>
#include <ngx_http_lifecycle_errno.h>


//static ngx_str_t rcs_name = ngx_string("rc server");
static ngx_str_t rs_name = ngx_string("root server");
static ngx_str_t ms_name = ngx_string("meta server");


ngx_int_t
ngx_http_lifecycle_peer_init(ngx_http_lifecycle_t *t)
{
    char                            *addr;
    uint16_t                         port;
    ngx_http_lifecycle_connection_pool_t      *conn_pool;
    ngx_http_lifecycle_peer_connection_t  *root_server, *meta_server;

    conn_pool = t->main_conf->conn_pool;

    t->lifecycle_peer_servers = ngx_pcalloc(t->pool,
        sizeof(ngx_http_lifecycle_peer_connection_t) * NGX_HTTP_LIFECYCLE_SERVER_COUNT);
    if (t->lifecycle_peer_servers == NULL) {
        return NGX_ERROR;
    }

    root_server = &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_ROOT_SERVER];
    meta_server = &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_META_SERVER];


    /* root server */
    root_server->peer.sockaddr = t->main_conf->krs_addr->sockaddr;
    root_server->peer.socklen = t->main_conf->krs_addr->socklen;
    root_server->peer.log = t->log;
    root_server->peer.name = &rs_name;
    root_server->peer.data = conn_pool;
    root_server->peer.get = conn_pool->get_peer;
    root_server->peer.free = conn_pool->free_peer;
    root_server->peer.log_error = NGX_ERROR_ERR;
    addr = inet_ntoa(((struct sockaddr_in*)
                      (root_server->peer.sockaddr))->sin_addr);
    port = ntohs(((struct sockaddr_in*)
                  (root_server->peer.sockaddr))->sin_port);
    ngx_sprintf(root_server->peer_addr_text, "%s:%d", addr, port);

    /* meta server */
    meta_server->peer.log = t->log;
    meta_server->peer.name = &ms_name;
    meta_server->peer.data = conn_pool;
    meta_server->peer.get = conn_pool->get_peer;
    meta_server->peer.free = conn_pool->free_peer;
    meta_server->peer.log_error = NGX_ERROR_ERR;

    t->lifecycle_peer_count = 2;

    return NGX_OK;
}



ngx_http_lifecycle_peer_connection_t *
ngx_http_lifecycle_select_peer(ngx_http_lifecycle_t *t)
{
    switch (t->r_ctx.action.code) {

    case NGX_HTTP_LIFECYCLE_ACTION_POST:
    case NGX_HTTP_LIFECYCLE_ACTION_PUT:
    case NGX_HTTP_LIFECYCLE_ACTION_GET:
    case NGX_HTTP_LIFECYCLE_ACTION_DEL:
        switch (t->state) {

        case NGX_HTTP_LIFECYCLE_STATE_ACTION_GET_META_TABLE:
            t->create_request = ngx_http_lifecycle_create_rs_request;
            t->process_request_body = ngx_http_lifecycle_process_rs;
            t->input_filter = NULL;
            return &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_ROOT_SERVER];

        case NGX_HTTP_LIFECYCLE_STATE_ACTION_PROCESS:
            t->create_request = ngx_http_lifecycle_create_ms_request;
            t->process_request_body = ngx_http_lifecycle_process_ms;
            t->input_filter = NULL;
            t->retry_handler = ngx_http_lifecycle_retry_ms;
            return &t->lifecycle_peer_servers[NGX_HTTP_LIFECYCLE_META_SERVER];

        case NGX_HTTP_LIFECYCLE_STATE_ACTION_DONE:
            t->input_filter = NULL;
            return t->lifecycle_peer;
        default:
            return NULL;
        }
        break;
    default:
        break;
    }

    return NULL;
}

