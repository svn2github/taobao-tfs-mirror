
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_lifecycle_common.h>
#include <ngx_http_lifecycle_restful.h>


#define NGX_HTTP_LIFECYCLE_VERSION1 "v1"
#define NGX_HTTP_LIFECYCLE_VERSION2 "v2"


static ngx_int_t
ngx_http_lifecycle_parse_raw(ngx_http_request_t *r,
    ngx_http_lifecycle_restful_ctx_t *ctx, u_char *data)
{
    u_char         *p, ch, *start, *last, *meta_data;

    enum {
        sw_appkey = 0,
        sw_metadata,
        sw_name,
    } state;

    state = sw_appkey;
    last = r->uri.data + r->uri.len;
    start = data;
    meta_data = NULL;

    for (p = data; p < last; p++) {
        ch = *p;

        switch (state) {

        case sw_appkey:
            if (ch == '/') {
                ctx->appkey.data = start;
                ctx->appkey.len = p - start;

                state = sw_metadata;
                if (p + 1 < last) {
                    meta_data = p + 1;
                }
            }

            break;

        case sw_metadata:
            if (ch == '/') {
                if (ngx_memcmp(meta_data, "metadata", 8) == 0) {
                    if (p + 1 < last) {
                        ctx->file_path_s.data = p + 1;
                        state = sw_name;

                    } else {
                        return NGX_ERROR;
                    }
                }
            }
        case sw_name:
            break;
        }
    }
    /* need sw_name */
    if (r->method == NGX_HTTP_GET || r->method == NGX_HTTP_DELETE
        || r->method == NGX_HTTP_PUT) {
        if (state == sw_appkey) {
            return NGX_ERROR;
        }

        if (state == sw_metadata) {
            ctx->file_path_s.data = meta_data;
        }

        if (state == sw_name) {
            if (r->method == NGX_HTTP_DELETE || r->method == NGX_HTTP_PUT) {
                return NGX_ERROR;
            }
        }
        ctx->file_path_s.len = p - ctx->file_path_s.data;
        if (ctx->file_path_s.len < 1
            || ctx->file_path_s.len > NGX_HTTP_LIFECYCLE_MAX_FILE_NAME_LEN)
        {
            return NGX_ERROR;
        }

    } else {
      /* POST */
        if (state == sw_appkey) {
            ctx->appkey.len = p - start;
            if (ctx->appkey.len == 0) {
                return NGX_ERROR;
            }
            ctx->appkey.data = start;

        } else {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_parse_custom_name(ngx_http_request_t *r,
    ngx_http_lifecycle_restful_ctx_t *ctx, u_char *data)
{
    u_char    *p, ch, *start, *last, *appid, *meta_data;
    ngx_int_t  rc;

    enum {
        sw_appkey = 0,
        sw_metadata,
        sw_appid,
        sw_uid,
        sw_type,
        sw_name,
    } state;

    state = sw_appkey;
    last = r->uri.data + r->uri.len;
    start = data;
    appid = NULL;
    meta_data = NULL;

    for (p = data; p < last; p++) {
        ch = *p;

        switch (state) {
        case sw_appkey:
            if (ch == '/') {
                ctx->appkey.data = start;
                ctx->appkey.len = p - start;

                start = p + 1;
                /* GET /v2/appkey/appid */
                if (start < last) {
                    if (*start == 'a') {
                        state = sw_name;
                        appid = start;
                    } else if (*start == 'm') {
                        state = sw_metadata;
                        meta_data = start;
                    } else {
                        state = sw_appid;
                    }
                }
            }
            break;
        case sw_metadata:
            if (ch == '/') {
                if (ngx_memcmp(meta_data, "metadata", 8) == 0) {
                    if (p + 1 < last) {
                        start = p + 1;
                        state = sw_appid;

                    } else {
                        return NGX_ERROR;
                    }
                }
            }
            break;
        case sw_appid:
            if (ch == '/') {
                rc = ngx_http_lifecycle_atoull(start, p - start,
                                         (unsigned long long *)&ctx->app_id);
                if (rc == NGX_ERROR || ctx->app_id == 0) {
                    return NGX_ERROR;
                }

                start = p + 1;
                state = sw_uid;
                break;
            }

            if (ch < '0' || ch > '9') {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                              "appid is invalid");
                return NGX_ERROR;
            }

            if ((size_t) (p - start) > (NGX_INT64_LEN - 1)) {
                return NGX_ERROR;
            }

            break;
        case sw_uid:
            if (ch == '/') {
                rc = ngx_http_lifecycle_atoull(start, p - start,
                                         (unsigned long long *)&ctx->user_id);
                if (rc == NGX_ERROR || ctx->user_id == 0) {
                    return NGX_ERROR;
                }
                start = p + 1;
                state = sw_type;
                break;
            }

            if (ch < '0' || ch > '9') {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                              "userid is invalid");
                return NGX_ERROR;
            }

            if ((size_t) (p - start) > NGX_INT64_LEN - 1) {
                ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                              "userid is too big");
                return NGX_ERROR;
            }
            break;
        case sw_type:
            if (ch == '/') {
                if (ngx_strncmp(start, "file", p - start) == 0) {
                    ctx->file_type = NGX_HTTP_LIFECYCLE_CUSTOM_FT_FILE;

                } else if (ngx_strncmp(start, "dir", p - start) == 0) {
                    ctx->file_type = NGX_HTTP_LIFECYCLE_CUSTOM_FT_DIR;

                } else {
                    return NGX_ERROR;
                }
                ctx->file_path_s.data = p;
                state = sw_name;
            }
            break;
        case sw_name:
            break;
        }
    }


    ctx->file_path_s.len = p - ctx->file_path_s.data;
    // TODO: trim whitespace && adjacent '/' && tailing '/'
    if (ctx->file_path_s.len < 1
        || ctx->file_path_s.len > NGX_HTTP_LIFECYCLE_MAX_FILE_NAME_LEN)
    {
        return NGX_ERROR;
    }

    /* forbid file actions on "/" */
    if (ctx->file_type == NGX_HTTP_LIFECYCLE_CUSTOM_FT_FILE
        && ctx->file_path_s.len == 1)
    {
        return NGX_ERROR;
    }

    return NGX_OK;
}




static ngx_int_t
ngx_http_lifecycle_parse_action(ngx_http_request_t *r,
    ngx_http_lifecycle_restful_ctx_t *ctx)
{
    ngx_int_t  rc;
    ngx_str_t  arg_value, file_path_d;

    switch(r->method) {
    case NGX_HTTP_GET:
    /* GET /lifecycle/v2/appkey/appid/uid/file/tfsname */
        ctx->file_name = ctx->file_path_s;
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_GET;
        ngx_str_set(&ctx->action.msg, "get_life_cycle");

        if (r->headers_in.range != NULL) {
            return NGX_HTTP_BAD_REQUEST;
        }
        break;

    case NGX_HTTP_POST:
    /* POST /v2/appkey/appid/uid/file/tfsname?
     * positive_expiretime=D2013-12-12T03:05:58 */
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_POST;
        ngx_str_set(&ctx->action.msg, "post_life_cycle");
        break;

    case NGX_HTTP_PUT:
    /* PUT /lifecycle/v2/appkey/appid/uid/file_path?
     * positive_expiretime=D2013-12-12T03:05:58 */
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_PUT;
        ngx_str_set(&ctx->action.msg, "put_life_cycle");
        break;

    case NGX_HTTP_DELETE:
    /* DELETE */
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_DEL;
        ngx_str_set(&ctx->action.msg, "del_life_cycle");
        break;

    default:
        return NGX_HTTP_BAD_REQUEST;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_parse_action_raw(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx)
{
    ngx_int_t       rc;
    ngx_str_t       arg_value;

    switch(r->method) {
    case NGX_HTTP_GET:
        /* GET /lifecycle/v1/appkey/TFS_NAME HTTP/1.1 */
        ctx->file_name = ctx->file_path_s;
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_GET;
        ngx_str_set(&ctx->action.msg, "get_life_cycle");
        return NGX_OK;

    case NGX_HTTP_POST:

        /* POST /v1/appkey?positive_expiretime=1234567 */
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_POST;
        ngx_str_set(&ctx->action.msg, "post_life_cycle");

        if (NGX_HTTP_LIFECYCLE_POSITIVE_TIME == ctx->expiretime_type) {

            if (ngx_http_arg(r, positive_expiretime_str,
                ngx_strlen(positive_expiretime_str), &arg_value) == NGX_OK)
            {
                if (arg_value.len != 0) {
                    ctx->absolute_time = ngx_http_lifecycle_get_positive_time(&arg_value);
                    if (ctx->absolute_time == NGX_ERROR) {
                        return NGX_HTTP_BAD_REQUEST;
                    }
                } else {
                    return NGX_HTTP_BAD_REQUEST;
                }
            } else {
                return NGX_HTTP_BAD_REQUEST;
            }
        } else if (NGX_HTTP_LIFECYCLE_RELATIVE_TIME == ctx->expiretime_type) {

            if (ngx_http_arg(r, relative_expiretime_str,
                ngx_strlen(relative_expiretime_str), &arg_value) == NGX_OK)
            {
                if (arg_value.len != 0) {
                    ctx->absolute_time = 0;
                    ctx->absolute_time = ngx_http_lifecycle_get_relative_time(&arg_value);
                    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                                  "=====relative time: %d", ctx->absolute_time);
                    if (ctx->absolute_time == NGX_ERROR) {
                        return NGX_HTTP_BAD_REQUEST;
                    } else {
                        ctx->absolute_time += ngx_time();
                    }
                } else {
                    return NGX_HTTP_BAD_REQUEST;
                }
            } else {
                return NGX_HTTP_BAD_REQUEST;
            }
        } else {
            return NGX_HTTP_BAD_REQUEST;
        }
        return NGX_OK;

    case NGX_HTTP_DELETE:
        /* DELETE /v1/12345/T29RETBgVv1RymAV6K HTTP/1.1 */
        ctx->file_name = ctx->file_path_s;
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_DEL;
        ngx_str_set(&ctx->action.msg, "del_life_cycle");
        return NGX_OK;

    case NGX_HTTP_PUT:
        /* PUT /lifecycle/v1/appkey/TFS_NAME?positive_expiretime=12345 HTTP/1.1 */
        ctx->action.code = NGX_HTTP_LIFECYCLE_ACTION_PUT;
        ngx_str_set(&ctx->action.msg, "put_life_cycle");

        if (NGX_HTTP_LIFECYCLE_POSITIVE_TIME == ctx->expiretime_type) {

            if (ngx_http_arg(r, positive_expiretime_str,
                ngx_strlen(positive_expiretime_str), &arg_value) == NGX_OK)
            {
                if (arg_value.len != 0) {
                    ctx->absolute_time = ngx_http_lifecycle_get_positive_time(&arg_value);
                    if (ctx->absolute_time == NGX_ERROR) {
                        return NGX_HTTP_BAD_REQUEST;
                    }
                } else {
                    return NGX_HTTP_BAD_REQUEST;
                }
            } else {
                return NGX_HTTP_BAD_REQUEST;
            }
        } else if (NGX_HTTP_LIFECYCLE_RELATIVE_TIME == ctx->expiretime_type) {

            if (ngx_http_arg(r, relative_expiretime_str,
                ngx_strlen(relative_expiretime_str), &arg_value) == NGX_OK)
            {
                if (arg_value.len != 0) {
                    ctx->absolute_time = 0;
                    ctx->absolute_time = ngx_http_lifecycle_get_relative_time(&arg_value);
                    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                                  "=====relative time: %d", ctx->absolute_time);
                    if (ctx->absolute_time == NGX_ERROR) {
                        return NGX_HTTP_BAD_REQUEST;
                    }
                } else {
                    return NGX_HTTP_BAD_REQUEST;
                }
            } else {
                return NGX_HTTP_BAD_REQUEST;
            }
        } else {
            return NGX_HTTP_BAD_REQUEST;
        }
        if (ctx->file_path_s.data == NULL) {
            return NGX_HTTP_BAD_REQUEST;
        }
        ctx->file_name = ctx->file_path_s;
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "======^^^^^^^===== %d",  ctx->file_name.len);
        return NGX_OK;
    default:
        return NGX_HTTP_BAD_REQUEST;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_parse_uri(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx)
{
    u_char         *p, ch, *last;

    enum {
        sw_start = 0,
        sw_lifecycle,
        sw_version_prefix,
        sw_version,
        sw_backslash,
    } state;

    state = sw_start;
    last = r->uri.data + r->uri.len;

    for (p = r->uri.data; p < last; p++) {
        ch = *p;

        switch (state) {
        case sw_start:
            state = sw_lifecycle;
            break;

        case sw_lifecycle:
            if (ngx_memcmp(p, "lifecycle", 9) == 0) {
              ctx->lifecycle_on = 1;
              p = p + 9;
            } else {
              p--;
            }
            state = sw_version_prefix;
            break;

        case sw_version_prefix:
            if (ch != 'v') {
                ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                               "version invalid %V ", &r->uri);
                return NGX_ERROR;
            }
            state = sw_version;
            break;

        case sw_version:
            if (ch < '1' || ch > '9') {
                return NGX_ERROR;
            }

            ctx->version = ch - '0';
            if (ctx->version > 2) {
                return NGX_ERROR;
            }

            state = sw_backslash;
            break;

        case sw_backslash:
            if (ch != '/') {
                return NGX_ERROR;
            }

            if (ctx->version == 1) {
                return ngx_http_lifecycle_parse_raw(r, ctx, ++p);
            }

            if (ctx->version == 2) {
                return ngx_http_lifecycle_parse_custom_name(r, ctx, ++p);
            }

            return NGX_ERROR;
        }
    }

    return NGX_ERROR;
}


ngx_int_t
ngx_http_lifecycle_on_off(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx)
{
    ngx_str_t            arg_value;
    ngx_int_t            rc;

    if (r->method == NGX_HTTP_POST || r->method == NGX_HTTP_PUT) {

        if (ngx_http_arg(r, positive_expiretime_str, ngx_strlen(positive_expiretime_str),
            &arg_value) == NGX_OK)
        {
            ctx->expiretime_type = NGX_HTTP_LIFECYCLE_POSITIVE_TIME;
            if (arg_value.len != 0) {
                ctx->lifecycle_on = 1;
            }
        } else if (ngx_http_arg(r, relative_expiretime_str, ngx_strlen(relative_expiretime_str),
                                &arg_value) == NGX_OK)
        {
            ctx->expiretime_type = NGX_HTTP_LIFECYCLE_RELATIVE_TIME;
            if (arg_value.len != 0) {
                ctx->lifecycle_on = 1;
            }

        } else {
            ctx->lifecycle_on = 0;
        }
    } else if (r->method == NGX_HTTP_DELETE) {
        ctx->lifecycle_on = 1;
    }
    rc = ngx_http_lifecycle_parse_uri(r, ctx);

    ngx_log_error(NGX_LOG_INFO,  r->connection->log, 0,
                  "!!!!!!!lifecycle_on is: %d and rc is %d", ctx->lifecycle_on, rc);
    return rc;
}

ngx_int_t
ngx_http_lifecycle_parse(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx)
{
    ngx_int_t            rc;

    rc = ngx_http_lifecycle_on_off(r, ctx);

    if (ctx->lifecycle_on) {

        if (rc == NGX_ERROR) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "parse uri failed");
            return NGX_HTTP_BAD_REQUEST;
        }

        if (ctx->version == 1) {
            rc = ngx_http_lifecycle_parse_action_raw(r, ctx);
        }

        if (ctx->version == 2) {
            rc = ngx_http_lifecycle_parse_action(r, ctx);
        }
    }

    return rc;
}
