
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#include <ngx_http_tfs_serialization.h>
#include <ngx_http_tfs_kv_root_server_message.h>


ngx_chain_t *
ngx_http_tfs_root_server_create_message(ngx_pool_t *pool)
{
    ngx_buf_t                  *b;
    ngx_chain_t                *cl;
    ngx_http_tfs_kv_rs_request_t  *req;

    b = ngx_create_temp_buf(pool, sizeof(ngx_http_tfs_kv_rs_request_t));
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_kv_rs_request_t *) b->pos;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.len = sizeof(uint8_t);
    req->header.type = NGX_HTTP_TFS_REQ_KV_RT_GET_TABLE_MESSAGE;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       req->header.len);
    req->header.id = ngx_http_tfs_generate_packet_id();

    b->last += sizeof(ngx_http_tfs_kv_rs_request_t);

    cl = ngx_alloc_chain_link(pool);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = b;
    cl->next = NULL;

    return cl;
}


ngx_int_t
ngx_http_tfs_root_server_parse_message(ngx_http_tfs_t *t)
{
    u_char                          *p;
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_http_tfs_header_t           *header;
    ngx_int_t                        rc;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    type = header->type;
    tp = t->tfs_peer;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "get table (kv root server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    p = tp->body_buffer.pos;
    rc = ngx_http_tfs_deserialize_kv_meta_table(&p, &t->loc_conf->meta_server_table);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "deserialize kv meta table failed.");
        return NGX_ERROR;
    }

    t->loc_conf->meta_server_table.valid = NGX_HTTP_TFS_YES;
    t->loc_conf->meta_access_count = 0;
    t->loc_conf->meta_fail_count = 0;

    return NGX_OK;
}
