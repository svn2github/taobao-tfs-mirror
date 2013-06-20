
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#include <ngx_tfs_common.h>
#include <ngx_http_tfs_restful.h>


#define NGX_HTTP_TFS_VERSION1 "v1"
#define NGX_HTTP_TFS_VERSION2 "v2"
#define MAX_HEADER_SIZE 8192
#define MAX_CUSTOM_META_SIZE 1024


/* need find from header */

static ngx_str_t s3_authorization = ngx_string("Authorization");
static ngx_str_t s3_content_md5 = ngx_string("Content-MD5");
static ngx_str_t s3_date = ngx_string("Date");
static ngx_str_t s3_custom_header = ngx_string("x-tfs-");
static ngx_str_t s3_custom_meta_header = ngx_string("x-tfs-meta-");


ngx_int_t
ngx_http_find_custom_head(ngx_http_request_t *r, ngx_str_t *name_custom,
    ngx_str_t *name_meta, ngx_array_t *custom_head,
    ngx_uint_t *sum_custom_header_len, ngx_uint_t *sum_meta_header_len)
{
    /* custom_head */
    ngx_str_t *custom_array;

    /* r list */
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
        /* if is x-tfs-acl */
        if (name_custom->len <= header[i].key.len) {
            if (ngx_strncasecmp(header[i].key.data, name_custom->data,
                                name_custom->len) == 0)
            {
                custom_array = ngx_array_push(custom_head);
                custom_array->data = header[i].key.data;
                custom_array->len = header[i].key.len;
                *sum_custom_header_len += header[i].key.len;

                custom_array = ngx_array_push(custom_head);
                custom_array->data = header[i].value.data;
                custom_array->len = header[i].value.len;
                *sum_custom_header_len += header[i].value.len;

                /* : and \n */
                *sum_custom_header_len += 2;
                /*if is x-tfs-meta-like*/
                if (ngx_strncasecmp(header[i].key.data, name_meta->data,
                                    name_meta->len) == 0)
                {
                    *sum_meta_header_len += header[i].key.len;
                    *sum_meta_header_len += header[i].value.len;
                    if (*sum_meta_header_len > MAX_CUSTOM_META_SIZE) {
                        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                            "sum_meta_header_len is %d out of max",
                            *sum_meta_header_len);
                        return NGX_HTTP_BAD_REQUEST;
                    }
                }

                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "head is %V and %V ",
                              &(header[i].key), &(header[i].value));

            }
        }
    }
    return NGX_OK;
}



ngx_int_t
ngx_http_restful_parse_authorize(ngx_http_request_t *r,
    ngx_http_tfs_restful_ctx_t *ctx)
{
    ngx_int_t       rc;
    ngx_str_t       authorization;

    u_char *start, *last, *p ,ch;

    rc = ngx_http_tfs_parse_headerin(r, &s3_authorization, &authorization);
    if (rc == NGX_DECLINED) {
        return NGX_ERROR;
    }

    start = authorization.data;
    last  = authorization.data + authorization.len;

    if ( *(start) == 'T' && *(start+1) == 'F' &&
         *(start+2) == 'S' && *(start+3) == ' ')
    {
        start += 4;
        for (p = start; p < last; p++) {
            ch = *p;
            if (ch == ':') {
                break;
            }
        }

        ctx->access_id.data = start;
        ctx->access_id.len  = p - start;

        ctx->signature_user.data = p + 1;
        ctx->signature_user.len = last - p - 1;

    }
    return NGX_OK;
}


ngx_int_t
ngx_http_restful_parse_bucket(ngx_http_request_t *r,
    ngx_http_tfs_restful_ctx_t *ctx)
{
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "now in parse bucket");
    ngx_int_t            rc;
    u_char              *p, ch, *start, *last;

    if (r->headers_in.host != NULL) {
        start = r->headers_in.host->value.data;

        last = r->headers_in.host->value.data +
               r->headers_in.host->value.len;
    }
    else {
        rc = NGX_ERROR;
        ngx_log_error(NGX_LOG_ERR,  r->connection->log, 0, "host is null");
        return rc;
    }

    for (p = start; p < last; p++) {
        ch = *p;
        if (ch == '.') {
            ctx->bucket_name.data = start;
            ctx->bucket_name.len = p - start;
            break;
        }
    }
    /*TODO FIX ME*/
//    ngx_str_set(&ctx->bucket_name, "tititi");
    return NGX_OK;
}


ngx_int_t
ngx_http_restful_parse_object(ngx_http_request_t *r,
    ngx_http_tfs_restful_ctx_t *ctx)
{

    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "now in parse object");
    u_char         *p, ch, *last, *start, *object;

    enum {
        sw_backslash = 0,
        sw_object,
        sw_end,
    } state;

    state = sw_backslash;
    last = r->uri.data + r->uri.len;
    start = r->uri.data;
    object = start;

    for (p = r->uri.data; p < last; p++) {
        ch = *p;
        switch (state) {
        case sw_backslash:
            if (ch == '/') {
                state = sw_object;
                object = p + 1;
            }
            break;
        case sw_object:
            if (ch == '?') {
                ctx->object_name.data = object;
                ctx->object_name.len = p - object;
                state = sw_end;
            }
            break;
        case sw_end:
            break;
        }
    }

    if (state == sw_object) {
        ctx->object_name.data = object;
        ctx->object_name.len = p - object;
        state = sw_end;
    }

    if (ctx->object_name.len == 0) {
        ctx->file_type = NGX_HTTP_TFS_CUSTOM_FT_DIR;
    } else {
        ctx->file_type = NGX_HTTP_TFS_CUSTOM_FT_FILE;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_s3_parse_action(ngx_http_request_t *r,
    ngx_http_tfs_restful_ctx_t *ctx)
{
    ngx_int_t  rc;
    ngx_str_t  arg_value;

    switch(r->method) {
    case NGX_HTTP_GET:

        if (ctx->file_type == NGX_HTTP_TFS_CUSTOM_FT_DIR) {
            ctx->action.code = NGX_HTTP_TFS_ACTION_GET_BUCKET;
            ngx_str_set(&ctx->action.msg, "get_bucket");
            return NGX_OK;
        }

        ctx->action.code = NGX_HTTP_TFS_ACTION_GET_OBJECT;
        ngx_str_set(&ctx->action.msg, "get_object");

        if (r->headers_in.range != NULL) {
            return NGX_HTTP_BAD_REQUEST;
        }

        if (ngx_http_arg(r, (u_char *) "check_hole", 10, &arg_value) == NGX_OK) {
            ctx->chk_file_hole = ngx_atoi(arg_value.data, arg_value.len);
            if (ctx->chk_file_hole == NGX_ERROR
                || (ctx->chk_file_hole != NGX_HTTP_TFS_NO
                    && ctx->chk_file_hole != NGX_HTTP_TFS_YES))
            {
                return NGX_HTTP_BAD_REQUEST;
            }
        }

        if (ngx_http_arg(r, (u_char *) "offset", 6, &arg_value) == NGX_OK) {
            ctx->offset = ngx_atoll(arg_value.data, arg_value.len);
            if (ctx->offset == NGX_ERROR) {
                return NGX_HTTP_BAD_REQUEST;
            }
        }

        if (ngx_http_arg(r, (u_char *) "size", 4, &arg_value) == NGX_OK) {
            rc = ngx_http_tfs_atoull(arg_value.data, arg_value.len,
                                     (unsigned long long *)&ctx->size);
            if (rc == NGX_ERROR) {
                return NGX_HTTP_BAD_REQUEST;
            }

            // FIXME: return 0 ?
            if (ctx->size == 0) {
                return NGX_HTTP_BAD_REQUEST;
            }
            return NGX_OK;
        }

        ctx->size = NGX_HTTP_TFS_MAX_SIZE;
        break;

    case NGX_HTTP_POST:

        ctx->action.code = NGX_HTTP_TFS_ACTION_PUT_OBJECT;
        ngx_str_set(&ctx->action.msg, "put_object");
        break;

    case NGX_HTTP_PUT:
        if (ctx->file_type == NGX_HTTP_TFS_CUSTOM_FT_DIR) {
            ctx->action.code = NGX_HTTP_TFS_ACTION_PUT_BUCKET;
            ngx_str_set(&ctx->action.msg, "put_bucket");
            break;
        }

        ctx->action.code = NGX_HTTP_TFS_ACTION_PUT_OBJECT;
        ngx_str_set(&ctx->action.msg, "put_object");
        if (ngx_http_arg(r, (u_char *) "offset", 6, &arg_value) == NGX_OK) {
            ctx->offset = ngx_atoll(arg_value.data, arg_value.len);
            if (ctx->offset == NGX_ERROR) {
                return NGX_HTTP_BAD_REQUEST;
            }

        } else {
            /* no specify offset, append by default */
            ctx->offset = NGX_HTTP_TFS_APPEND_OFFSET;
        }
        break;

    case NGX_HTTP_DELETE:
        /* deprecated */
        if (ctx->file_type == NGX_HTTP_TFS_CUSTOM_FT_DIR) {
            ctx->action.code = NGX_HTTP_TFS_ACTION_DEL_BUCKET;
            ngx_str_set(&ctx->action.msg, "del_bucket");
            break;
        }

        ctx->action.code = NGX_HTTP_TFS_ACTION_DEL_OBJECT;
        ngx_str_set(&ctx->action.msg, "del_object");
        /* for t->file.left_length */
        ctx->size = NGX_HTTP_TFS_MAX_SIZE;
        break;

    case NGX_HTTP_HEAD:
        /* deprecated */
        if (ctx->file_type == NGX_HTTP_TFS_CUSTOM_FT_DIR) {
            ctx->action.code = NGX_HTTP_TFS_ACTION_HEAD_BUCKET;
            ngx_str_set(&ctx->action.msg, "head_bucket");
            break;
        }

        ctx->action.code = NGX_HTTP_TFS_ACTION_HEAD_OBJECT;
        ngx_str_set(&ctx->action.msg, "head_object");

        break;

    default:
        return NGX_HTTP_BAD_REQUEST;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_s3_parse(ngx_http_request_t *r, ngx_http_tfs_restful_ctx_t *ctx)
{
    ngx_int_t            rc;
    /* fix appkey for rc*/
    ngx_str_set(&ctx->appkey, NGX_HTTP_TFS_NGINX_APPKEY);

    ctx->app_id = 1;
    ctx->version = 2;

    ctx->custom_head = ngx_array_create(r->pool, 50, sizeof(ngx_str_t));

    /* parse_bucket */
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0, "now in parse s3");

    rc = ngx_http_restful_parse_bucket(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "parse bucket failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "bucket_name is: %V", &ctx->bucket_name);
    /* parse_object */
    rc = ngx_http_restful_parse_object(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "parse object failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "object_name is: %V", &ctx->object_name);

    /* parse_authorize */
    rc = ngx_http_restful_parse_authorize(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "parse authorize failed");
        return NGX_HTTP_BAD_REQUEST;
    }
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_access_id is: %V", &ctx->access_id);
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_signature_user is: %V", &ctx->signature_user);

    /* parse_method */
    ctx->method.data = r->method_name.data;
    ctx->method.len = r->method_name.len;
    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_method is: %V", &ctx->method);

    /* parse_content_md5 */
    rc = ngx_http_tfs_parse_headerin(r, &s3_content_md5, &ctx->content_md5);
    if (rc == NGX_OK) {
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_content_md5 is: %V", &ctx->content_md5);
    }
    /* parse_content_type */
    if (r->headers_in.content_type != NULL) {
        ctx->content_type.data = r->headers_in.content_type->value.data;
        ctx->content_type.len = r->headers_in.content_type->value.len;
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_content_type is: %V", &ctx->content_type);
    }
    /* parse_date */
    rc = ngx_http_tfs_parse_headerin(r, &s3_date, &ctx->date);
    if (rc == NGX_OK) {
        ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "s3_date is: %V", &ctx->date);
    }


    /* parse_custom_head*/
    ngx_uint_t sum_custom_header_len = 0;
    ngx_uint_t sum_meta_header_len = 0;
    rc = ngx_http_find_custom_head(r, &s3_custom_header, &s3_custom_meta_header,
                                   ctx->custom_head, &sum_custom_header_len,
                                   &sum_meta_header_len);
    ctx->custom_head_len = sum_custom_header_len;

    /* parse action */
    rc = ngx_http_s3_parse_action(r, ctx);

    return rc;
}


ngx_int_t
ngx_http_restful_parse(ngx_http_request_t *r, ngx_http_tfs_restful_ctx_t *ctx)
{
    ngx_int_t            rc;
/*
    rc = ngx_http_restful_parse_uri(r, ctx);
    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse uri failed");
        return NGX_HTTP_BAD_REQUEST;
    }


    if (ctx->version == 2) {
        rc = ngx_http_restful_parse_action(r, ctx);
    }
*/
    rc = ngx_http_s3_parse(r, ctx);
    return rc;
}
