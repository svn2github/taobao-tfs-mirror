
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#include <ngx_http_tfs_root_server_message.h>
#include <zlib.h>


ngx_chain_t *
ngx_http_tfs_root_server_create_message(ngx_pool_t *pool)
{
    ngx_buf_t                  *b;
    ngx_chain_t                *cl;
    ngx_http_tfs_rs_request_t  *req;

    b = ngx_create_temp_buf(pool, sizeof(ngx_http_tfs_rs_request_t));
    if (b == NULL) {
        return NULL;
    }

    req = (ngx_http_tfs_rs_request_t *) b->pos;
    req->header.flag = NGX_HTTP_TFS_PACKET_FLAG;
    req->header.len = sizeof(uint8_t);
    req->header.type = NGX_HTTP_TFS_REQ_RT_GET_TABLE_MESSAGE;
    req->header.version = NGX_HTTP_TFS_PACKET_VERSION;
    req->header.crc = ngx_http_tfs_crc(NGX_HTTP_TFS_PACKET_FLAG,
                                       (const char *) (&req->header + 1),
                                       req->header.len);
    req->header.id = ngx_http_tfs_generate_packet_id();

    b->last += sizeof(ngx_http_tfs_rs_request_t);

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
    uLongf                           table_length;
    uint16_t                         type;
    ngx_str_t                        action;
    ngx_int_t                        rc;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_rs_response_t      *resp;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    type = header->type;
    tp = t->tfs_peer;

    switch (type) {
    case NGX_HTTP_TFS_STATUS_MESSAGE:
        ngx_str_set(&action, "get table (root server)");
        return ngx_http_tfs_status_message(&tp->body_buffer, &action, t->log);
    }

    resp = (ngx_http_tfs_rs_response_t *) (tp->body_buffer.pos);
    table_length = NGX_HTTP_TFS_METASERVER_COUNT * sizeof(uint64_t);

    rc = uncompress((Bytef *) (t->loc_conf->meta_server_table.table),
                    &table_length, resp->table, resp->length);
    if (rc != Z_OK) {
        ngx_log_error(NGX_LOG_ERR, t->log, errno, "uncompress error");
        return NGX_ERROR;
    }

    t->loc_conf->meta_server_table.version = resp->version;

    return NGX_OK;
}
