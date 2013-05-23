#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_http_s3_parse.h>
#include <ngx_http_hmac_sha1.h>

typedef struct {
    ngx_flag_t enable;
} ngx_http_authorize_loc_conf_t;


static char *ngx_http_authorize(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_authorize_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_authorize_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);

static ngx_command_t ngx_http_authorize_commands[] = {
    {
        ngx_string("s3_authorize"),
        NGX_HTTP_LOC_CONF|NGX_HTTP_LIF_CONF|NGX_CONF_TAKE12,
        ngx_http_authorize,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_authorize_loc_conf_t, enable),
        NULL
    },
    ngx_null_command
};

static ngx_http_module_t ngx_http_authorize_module_ctx = {
    NULL,
    NULL,

    NULL,
    NULL,

    NULL,
    NULL,

    ngx_http_authorize_create_loc_conf,
    ngx_http_authorize_merge_loc_conf

};

ngx_module_t ngx_http_authorize_module = {
    NGX_MODULE_V1,
    &ngx_http_authorize_module_ctx,
    ngx_http_authorize_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};

static ngx_int_t
ngx_http_s3_construct_str_to_sign(ngx_http_request_t *r, ngx_http_s3_parse_ctx_t *t, ngx_buf_t *str_sign)
{
    /**/
    if (t->method.len > 0) {
        memcpy(str_sign->last, t->method.data, t->method.len);
        str_sign->last += t->method.len;
    }
    memcpy(str_sign->last, "\n", 1);
    str_sign->last += 1;

    if (t->content_md5.len > 0) {
        memcpy(str_sign->last, t->content_md5.data, t->content_md5.len);
        str_sign->last += t->content_md5.len;
    }
    memcpy(str_sign->last, "\n", 1);
    str_sign->last += 1;

    if (t->content_type.len > 0) {
        memcpy(str_sign->last, t->content_type.data, t->content_type.len);
        str_sign->last += t->content_type.len;
    }
    memcpy(str_sign->last, "\n", 1);
    str_sign->last += 1;

    if (t->date.len > 0) {
        memcpy(str_sign->last, t->date.data, t->date.len);
        str_sign->last += t->date.len;
    }
    memcpy(str_sign->last, "\n", 1);
    str_sign->last += 1;

    /*Constructing the CanonicalizedResource Element*/
    memcpy(str_sign->last, "/", 1);
    str_sign->last += 1;
    memcpy(str_sign->last, t->bucket_name.data, t->bucket_name.len);
    str_sign->last += t->bucket_name.len;
    memcpy(str_sign->last, "/", 1);
    str_sign->last += 1;
    if (t->object_name.len > 0) {
        memcpy(str_sign->last, t->object_name.data, t->object_name.len);
        str_sign->last += t->object_name.len;
    }

    /*custom header TODO
    memcpy(str_sign->last, t->method.data, t->method.len);
    str_sign->last += t->method.len;
    memcpy(str_sign->last, "\n", 1);
    str_sign->last += 1;
    */
    return NGX_OK;
}

static ngx_int_t
ngx_http_authorize_is_equal_signature(ngx_http_request_t *r, ngx_http_s3_parse_ctx_t *t,
                                      ngx_buf_t *access_secret_key,
                                      ngx_buf_t *str_to_sign, ngx_buf_t *signature_kv)
{
  unsigned int signature_kv_len;

  hmac_sha1(access_secret_key->pos, access_secret_key->last - access_secret_key->pos,
            str_to_sign->pos, str_to_sign->last - str_to_sign->pos,
            signature_kv->last, &signature_kv_len);

  signature_kv->last = signature_kv->pos + signature_kv_len;

  ngx_int_t cmp_ret = 0;

  if (signature_kv_len == t->signature_user.len)
  {
      cmp_ret = ngx_strncmp(t->signature_user.data, signature_kv->pos, signature_kv_len);
      if (0 != cmp_ret)
      {
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "signature is diff signature_mykv:%V", &signature_kv->pos);
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "signature is diff signature_user:%V", &t->signature_user.data);
      }
      else
      {
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                        "signature is right signature_mykv:%V", &signature_kv->pos);
      }
  }
  else
  {
      ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                    "signature len is diff user:%d kv:%d", t->signature_user.len, signature_kv_len);
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "signature is diff signature_mykv:%V", &signature_kv->pos);
          ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "signature is diff signature_user:%V", &t->signature_user.data);
  }
  return cmp_ret;
}

static ngx_int_t
ngx_http_authorize_handler(ngx_http_request_t *r)
{
    ngx_int_t rc;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "======now reach ngx_http_authorize_handler");
    ngx_http_authorize_loc_conf_t *hlcf;
    hlcf = ngx_http_get_module_loc_conf(r, ngx_http_authorize_module);

    if (hlcf == NULL) {
        rc = NGX_ERROR;
        return rc;
    }

    if (hlcf->enable == 0) {
        rc = NGX_OK;
        //TODO
    }
    ngx_http_s3_parse_ctx_t t;
    /* malloc space access_secret_key */
    ngx_buf_t *access_secret_key = ngx_create_temp_buf(r->pool, 64);
    /* malloc space for signature_kv */
    ngx_buf_t *signature_kv = ngx_create_temp_buf(r->pool, 64);
    /* malloc space str_to_sign */
    ngx_buf_t *str_sign = ngx_create_temp_buf(r->pool, 1024);


    /* get INFO from headerin */
    rc = ngx_http_s3_parse(r, &t);
    if (rc != NGX_OK) {
        return rc;
    }
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "======now done ngx_http_s3_parse");
    /* TODO get access_secret_key from kv */
    memcpy(access_secret_key->last, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 40);
    access_secret_key->last = access_secret_key->pos + 40;


    /* construct_str_to_sign */
    rc = ngx_http_s3_construct_str_to_sign(r, &t, str_sign);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "======now done ngx_http_s3_construct_str_to_sign size is :%d", str_sign->last - str_sign->pos);
    if (rc != NGX_OK) {
        return rc;
    }
    rc = ngx_http_authorize_is_equal_signature(r, &t, access_secret_key, str_sign, signature_kv);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "======now done ngx_http_authorize_is_equal_signature");

    /* headout */
    return rc;
}

static void *
ngx_http_authorize_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_authorize_loc_conf_t *conf;


    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_authorize_loc_conf_t));
    if (conf == NULL)
    {
        return NGX_CONF_ERROR;
    }
    conf->enable = 0;

    return conf;
}

static char *
ngx_http_authorize_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_authorize_loc_conf_t *prev = parent;
    ngx_http_authorize_loc_conf_t *conf = child;

    ngx_conf_merge_off_value(conf->enable, prev->enable, 0);

    return NGX_CONF_OK;
}

static char *
ngx_http_authorize(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t *clcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_authorize_handler;

    ngx_conf_set_flag_slot(cf, cmd, conf);

    return NGX_CONF_OK;
}

