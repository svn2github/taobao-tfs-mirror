
/*
 * taobao tfs s3 for nginx
 *
 * This module is designed to support restful interface to tfs s3
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_tfs_common.h>
#include <ngx_http_tfs.h>
#include <ngx_http_tfs_restful.h>
#include <ngx_http_hmac_sha1.h>

#define MAX_SIGNATURE_SIZE 64
#define MAX_SECRET_KEY_SIZE 64
#define MAX_STR_TO_SIGN_SIZE 8192


static ngx_uint_t
ngx_http_get_total_sign_len(ngx_http_tfs_t *t)
{
    /**/
    ngx_uint_t str_sign_len = 0;

    str_sign_len += t->r_ctx.method.len;
    str_sign_len += 1;

    str_sign_len += t->r_ctx.content_md5.len;
    str_sign_len += 1;

    str_sign_len += t->r_ctx.content_type.len;
    str_sign_len += 1;

    str_sign_len += t->r_ctx.date.len;
    str_sign_len += 1;

    /*Constructing the CanonicalizedResource Element*/
    str_sign_len += 1;
    str_sign_len += t->r_ctx.bucket_name.len;
    str_sign_len += 1;
    str_sign_len += t->r_ctx.object_name.len;

    /*custom header*/
    str_sign_len += t->r_ctx.custom_head_len;

    return str_sign_len;
}

static ngx_int_t
ngx_http_s3_construct_str_to_sign(ngx_http_tfs_t *t,
    ngx_str_t *str_sign)
{
    /**/
    str_sign->len = 0;
    if (t->r_ctx.method.len > 0) {
        ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.method.data,
               t->r_ctx.method.len);
        str_sign->len += t->r_ctx.method.len;
    }
    ngx_memcpy(str_sign->data + str_sign->len, "\n", 1);
    str_sign->len += 1;

    if (t->r_ctx.content_md5.len > 0) {
        ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.content_md5.data,
               t->r_ctx.content_md5.len);
        str_sign->len += t->r_ctx.content_md5.len;
    }
    ngx_memcpy(str_sign->data + str_sign->len, "\n", 1);
    str_sign->len += 1;

    if (t->r_ctx.content_type.len > 0 ) {
        ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.content_type.data,
               t->r_ctx.content_type.len);
        str_sign->len += t->r_ctx.content_type.len;
    }
    ngx_memcpy(str_sign->data + str_sign->len, "\n", 1);
    str_sign->len += 1;

    if (t->r_ctx.date.len > 0 ) {
        ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.date.data, t->r_ctx.date.len);
        str_sign->len += t->r_ctx.date.len;
    }
    ngx_memcpy(str_sign->data + str_sign->len, "\n", 1);
    str_sign->len += 1;

    /*Constructing the CanonicalizedResource Element*/
    if (t->r_ctx.bucket_name.len > 0) {
      ngx_memcpy(str_sign->data + str_sign->len, "/", 1);
      str_sign->len += 1;
      ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.bucket_name.data,
          t->r_ctx.bucket_name.len);
      str_sign->len += t->r_ctx.bucket_name.len;
      ngx_memcpy(str_sign->data + str_sign->len, "/", 1);
      str_sign->len += 1;

      ngx_memcpy(str_sign->data + str_sign->len, t->r_ctx.object_name.data,
          t->r_ctx.object_name.len);
      str_sign->len += t->r_ctx.object_name.len;
    }

    /*custom header*/
    ngx_uint_t i;
    ngx_str_t *custom_array = t->r_ctx.custom_head->elts;
    for (i = 0; i + 1 < t->r_ctx.custom_head->nelts; i += 2) {

        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "x-tfs-key:%V",&custom_array[i] );
        ngx_memcpy(str_sign->data, custom_array[i].data, custom_array[i].len);
        str_sign->len += custom_array[i].len;
        ngx_memcpy(str_sign->data, ":", 1);
        str_sign->len += 1;

        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "x-tfs-value:%V",&custom_array[i+1] );
        ngx_memcpy(str_sign->data, custom_array[i+1].data, custom_array[i+1].len);
        str_sign->len += custom_array[i+1].len;
        ngx_memcpy(str_sign->data, "\n", 1);
        str_sign->len += 1;

    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_authorize_is_equal_signature(
    ngx_http_tfs_t *t, ngx_str_t *access_secret_key,
    ngx_str_t *str_to_sign, ngx_str_t *signature_kv)
{
  ngx_int_t rc = 0;
  ngx_uint_t signature_kv_len = 0;

  rc = hmac_sha1(access_secret_key->data, access_secret_key->len,
                 str_to_sign->data, str_to_sign->len,
                 signature_kv->data, MAX_SIGNATURE_SIZE, &signature_kv_len);

  if (rc == -1) {
    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                  "SIGNATURE SPACE IS NOT ENOUGH");
    return NGX_ERROR;
  }

  signature_kv->len = signature_kv_len;

  ngx_int_t cmp_ret = -1;

  if (signature_kv->len == t->r_ctx.signature_user.len) {

      cmp_ret = ngx_strncmp(t->r_ctx.signature_user.data, signature_kv->data,
                            signature_kv->len);
      if (0 != cmp_ret) {

          ngx_log_error(NGX_LOG_INFO, t->log, 0,
                        "signature is diff signature_mykv:%V",
                        signature_kv);
          ngx_log_error(NGX_LOG_INFO, t->log, 0,
                        "signature is diff signature_user:%V",
                        &t->r_ctx.signature_user);

      } else {

          ngx_log_error(NGX_LOG_INFO, t->log, 0,
                        "signature is right signature_mykv:%V",
                        signature_kv);
      }

  } else {

      ngx_log_error(NGX_LOG_INFO, t->log, 0,
                    "signature len is diff user:%d kv:%d",
                    t->r_ctx.signature_user.len, signature_kv->len);
      ngx_log_error(NGX_LOG_INFO, t->log, 0,
                    "signature is diff signature_mykv:%V",
                    signature_kv);
      ngx_log_error(NGX_LOG_INFO, t->log, 0,
                    "signature is diff signature_user:%V",
                    &t->r_ctx.signature_user);
  }

  return cmp_ret;
}

ngx_int_t
ngx_http_authorize_handler(ngx_http_tfs_t *t)
{
    ngx_int_t     rc;
    ngx_uint_t    total_sign_len;
    ngx_str_t     str_sign, access_secret_key, signature_kv;

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "======now reach ngx_http_authorize_handler");

    /* construct_str_to_sign */

    total_sign_len = ngx_http_get_total_sign_len(t);
    if (total_sign_len > MAX_STR_TO_SIGN_SIZE) {
        ngx_log_error(NGX_LOG_INFO, t->log, 0,
        "total_sign_len is %d out of space", total_sign_len);
        return NGX_HTTP_BAD_REQUEST;
    }
    /* malloc space str_to_sign */
    str_sign.data = ngx_palloc(t->pool, MAX_STR_TO_SIGN_SIZE);
    if (str_sign.data == NULL) {
        return NGX_ERROR;
    }

    rc = ngx_http_s3_construct_str_to_sign(t, &str_sign);
    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "==now done s3_construct_str_to_sign size is :%d",
                  str_sign.len);
    if (rc != NGX_OK) {
        return rc;
    }


    /* TODO get access_secret_key from kv */
    /* malloc space access_secret_key */
    access_secret_key.data = ngx_palloc(t->pool, MAX_SECRET_KEY_SIZE);
    if (access_secret_key.data == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(access_secret_key.data,
               t->authorize_info.access_secret_key.data, t->authorize_info.access_secret_key.len);
    access_secret_key.len = t->authorize_info.access_secret_key.len;


    /*check signature*/

    /* malloc space for signature_kv */
    signature_kv.data = ngx_palloc(t->pool, MAX_SIGNATURE_SIZE);
    if (signature_kv.data == NULL) {
        return NGX_ERROR;
    }
    rc = ngx_http_authorize_is_equal_signature(t, &access_secret_key,
                                               &str_sign, &signature_kv);
    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "==now done ngx_http_authorize_is_equal_signature");

    /* headout */
    return rc;
}

