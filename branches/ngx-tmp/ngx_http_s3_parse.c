
/*
 * taobao tfs s3 for nginx
 *
 * This module is designed to support restful interface to tfs s3
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_http_s3_parse.h>

/* need find from header */
static ngx_str_t s3_access_id = ngx_string("access-id");
static ngx_str_t s3_signature_user = ngx_string("signature-user");
static ngx_str_t s3_content_md5 = ngx_string("content-md5");
static ngx_str_t s3_date = ngx_string("date");
//static ngx_str_t s3_custom_header = ngx_string("x-tfs-");

ngx_int_t
ngx_http_find_head(ngx_http_request_t *r, ngx_str_t *name,
    ngx_str_t *value)
{
    ngx_list_part_t *part = &r->headers_in.headers.part;
    ngx_table_elt_t *header = part->elts;
    size_t i;
    for (i = 0; /* void */; i++) {

        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            header = part->elts;
            i = 0;
        }
        if (0 == header[i].hash) {
            continue;
        }
        //ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "QQ head is %V and %V ", &(header[i].key), &(header[i].value));
        if (name->len == header[i].key.len) {
            if (ngx_strncasecmp(header[i].key.data, name->data, name->len) == 0) {
                value->data = header[i].value.data;
                value->len = header[i].value.len;

               // ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "HAS FIND ");
                return NGX_OK;
            }
        }
    }
    return NGX_DECLINED;
}

ngx_int_t
ngx_http_restful_parse_bucket(ngx_http_request_t *r,
    ngx_http_s3_parse_ctx_t *ctx)
{
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0, "now in parse bucket");
    ngx_int_t            rc;
    ngx_int_t       flag = 0;
    u_char         *p, ch;
    u_char *start, *last;

    enum {
        sw_start = 0,
        sw_bucketname,
        sw_end,
    } state;

    state = sw_start;
    if (r->headers_in.host != NULL) {
        start = r->headers_in.host->value.data;

        last = r->headers_in.host->value.data + r->headers_in.host->value.len;
    }
    else {
        rc = NGX_ERROR;
        ngx_log_error(NGX_LOG_ERR,  r->connection->log, 0, "host is null");
        return rc;
    }

    for (p = start; p < last; p++) {
        ch = *p;

        switch (state) {
        case sw_start:
            if (ch == '.')
            state = sw_bucketname;
            break;
        case sw_bucketname:
            ctx->bucket_name.data = start;
            ctx->bucket_name.len = p - start - 1;
            state = sw_end;
            break;
        case sw_end:
            flag = 1;
            //TODO flag break;
            break;
        }
        if (flag == 1) {
            break;
        }
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_restful_parse_object(ngx_http_request_t *r, ngx_http_s3_parse_ctx_t *ctx)
{
    //ngx_int_t            rc;
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0, "now in parse object");
    u_char         *p, ch, *last, *start;

    enum {
        sw_start = 0,
        sw_object,
        sw_arg,
        sw_end,
    } state;

    state = sw_start;
    last = r->uri.data + r->uri.len;
    start = r->uri.data;

    for (p = r->uri.data; p < last; p++) {
        ch = *p;
        switch (state) {
        case sw_start:
            if (ch == '/') {
                start = p + 1;
                state = sw_object;
            }
            break;
        case sw_object:
            if (ch == '?') {
                ctx->object_name.data = start;
                ctx->object_name.len = p - start;

                start = p + 1;
                state = sw_arg;
                /* GET /photo/2013/5/22/419.gif */
            }
            break;
        case sw_arg:
            state = sw_end;
            break;
        case sw_end:
            break;
        }
    }
    if (state == sw_object)
    {
      ctx->object_name.data = start;
      ctx->object_name.len = p - start;
      state = sw_end;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_s3_parse(ngx_http_request_t *r, ngx_http_s3_parse_ctx_t *ctx)
{
    ngx_int_t            rc;
    ctx->bucket_name.len = 0;
    ctx->object_name.len = 0;
    ctx->access_id.len = 0;
    ctx->signature_user.len = 0;
    ctx->method.len = 0;
    ctx->content_md5.len = 0;
    ctx->content_type.len = 0;
    ctx->date.len = 0;

    /* */
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0, "now in parse s3");
    rc = ngx_http_restful_parse_bucket(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse bucket failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "bucket_name is: %V", &ctx->bucket_name);
    /* */
    rc = ngx_http_restful_parse_object(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse object failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "object_name is: %V", &ctx->object_name);
    /* */
    rc = ngx_http_find_head(r, &s3_access_id, &ctx->access_id);
    if (rc == -1) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse access failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_access_id is: %V", &ctx->access_id);
    /* */
    rc = ngx_http_find_head(r, &s3_signature_user, &ctx->signature_user);
    if (rc == -1) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse signature failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_signature_user is: %V", &ctx->signature_user);
    /* */
    ctx->method.data = r->method_name.data;
    ctx->method.len = r->method_name.len;
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_method is: %V", &ctx->method);
    /* */
    rc = ngx_http_find_head(r, &s3_content_md5, &ctx->content_md5);
    if (rc == NGX_OK) {
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_content_md5 is: %V", &ctx->content_md5);
    }
    /**/
    if (r->headers_in.content_type != NULL) {
        ctx->content_type.data = r->headers_in.content_type->value.data;
        ctx->content_type.len = r->headers_in.content_type->value.len;
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_content_type is: %V", &ctx->content_type);
    }
    /**/
    rc = ngx_http_find_head(r, &s3_date, &ctx->date);
    if (rc == NGX_OK) {
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_date is: %V", &ctx->date);
    }

    return NGX_OK;
}
