
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_core.h>
#include <ngx_event.h>
#include <ngx_http.h>
#include <ngx_config.h>
#include <ngx_http_lifecycle.h>


static void *ngx_http_lifecycle_create_main_conf(ngx_conf_t *cf);
static char *ngx_http_lifecycle_init_main_conf(ngx_conf_t *cf, void *conf);

static void *ngx_http_lifecycle_create_srv_conf(ngx_conf_t *cf);
static char *ngx_http_lifecycle_merge_srv_conf(ngx_conf_t *cf,
    void *parent, void *child);

static void *ngx_http_lifecycle_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_lifecycle_merge_loc_conf(ngx_conf_t *cf,
    void *parent, void *child);

static ngx_int_t ngx_http_lifecycle_module_init(ngx_cycle_t *cycle);

static ngx_int_t ngx_http_lifecycle_filter_init(ngx_conf_t *cf);

static char *ngx_http_lifecycle_pass(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_lifecycle_add_variable(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static char *ngx_http_lifecycle_kvroot_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_lifecycle_keepalive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *ngx_http_lifecycle_tackle_log(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_lifecycle_deprecated_oper_log(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_lifecycle_forbidden_oper_log(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static char *ngx_http_lifecycle_net_device(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);

static void ngx_http_lifecycle_read_body_handler(ngx_http_request_t *r);

static ngx_command_t  ngx_http_lifecycle_commands[] = {

    { ngx_string("lifecycle"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_http_lifecycle_pass,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("lifecycle_add_variable"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
      ngx_http_lifecycle_add_variable,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("lifecycle_net_device"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
      ngx_http_lifecycle_net_device,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("lifecycle_keepalive"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE2,
      ngx_http_lifecycle_keepalive,
      0,
      0,
      NULL },

    { ngx_string("lifecycle_tackle_log"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE12,
      ngx_http_lifecycle_tackle_log,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("lifecycle_deprecated_oper_log"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE12,
      ngx_http_lifecycle_deprecated_oper_log,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("lifecycle_ignore_client_abort"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_lifecycle_loc_conf_t, ignore_client_abort),
      NULL },

    { ngx_string("lifecycle_connect_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      0,
      offsetof(ngx_http_lifecycle_main_conf_t, lifecycle_connect_timeout),
      NULL },

    { ngx_string("lifecycle_send_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      0,
      offsetof(ngx_http_lifecycle_main_conf_t, lifecycle_send_timeout),
      NULL },

    { ngx_string("lifecycle_read_timeout"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      0,
      offsetof(ngx_http_lifecycle_main_conf_t, lifecycle_read_timeout),
      NULL },

    { ngx_string("lifecycle_buffer_size"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      0,
      offsetof(ngx_http_lifecycle_main_conf_t, buffer_size),
      NULL },

    { ngx_string("lifecycle_body_buffer_size"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      0,
      offsetof(ngx_http_lifecycle_main_conf_t, body_buffer_size),
      NULL },

    { ngx_string("lifecycle_intercept_errors"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_lifecycle_loc_conf_t, intercept_errors),
      NULL },

    { ngx_string("lifecycle_kvroot_server"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
      ngx_http_lifecycle_kvroot_server,
      0,
      0,
      NULL },

    { ngx_string("lifecycle_update_kmt_interval_count"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_lifecycle_loc_conf_t, update_kmt_interval_count),
      NULL },

    { ngx_string("lifecycle_update_kmt_fail_count"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_lifecycle_loc_conf_t, update_kmt_fail_count),
      NULL },


      ngx_null_command
};


static ngx_http_module_t  ngx_http_lifecycle_module_ctx = {
    NULL,                                        /* proconfiguration */
    ngx_http_lifecycle_filter_init,              /* postconfiguration */

    ngx_http_lifecycle_create_main_conf,         /* create main configuration */
    ngx_http_lifecycle_init_main_conf,           /* init main configuration */

    ngx_http_lifecycle_create_srv_conf,          /* create server configuration */
    ngx_http_lifecycle_merge_srv_conf,           /* merge server configuration */

    ngx_http_lifecycle_create_loc_conf,          /* create location configuration */
    ngx_http_lifecycle_merge_loc_conf            /* merge location configuration */
};


ngx_module_t  ngx_http_lifecycle_module = {
    NGX_MODULE_V1,
    &ngx_http_lifecycle_module_ctx,            /* module context */
    ngx_http_lifecycle_commands,               /* module directives */
    NGX_HTTP_MODULE,                           /* module type */
    NULL,                                      /* init master */
    NULL,                                      /* init module */
    NULL,                                      /* init process */
    NULL,                                      /* init thread */
    NULL,                                      /* exit thread */
    NULL,                                      /* exit process */
    NULL,                                      /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_http_output_header_filter_pt ngx_http_next_header_filter;
static ngx_http_output_body_filter_pt   ngx_http_next_body_filter;


/* alloc variable */
static ngx_int_t
ngx_http_lifecycle_on_off_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, sizeof(int32_t));

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_file_name_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, NGX_HTTP_LIFECYCLE_MAX_FILE_NAME_LEN);

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_version_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, sizeof(uint8_t));

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_expire_time_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, sizeof(int32_t));

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_app_key_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, NGX_HTTP_LIFECYCLE_MAX_APPKEY_LEN);

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_action_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, sizeof(uint16_t));

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_timetype_handler(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{

    v->data = ngx_palloc(r->pool, sizeof(uint8_t));

    if (v->data == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    return NGX_OK;
}


/* ====================== content phase ==============================*/
static ngx_int_t
ngx_http_lifecycle_content_handler(ngx_http_request_t *r)
{
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "=======into content phase_handler");
    ngx_int_t         rc;
    ngx_buf_t        *b;
    ngx_chain_t       out;


    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_type_len = sizeof("text/plain") - 1;
    ngx_str_set(&r->headers_out.content_type, "text/plain");

    rc = ngx_http_send_header(r);
    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    b = ngx_create_temp_buf(r->pool, sizeof("text/plain"));
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_memcpy(b->pos, (char *)"text/plain", sizeof("text/plain") - 1);
    b->last = b->pos + sizeof("text/plain") - 1;
    b->last_buf = 1;
    out.buf = b;
    out.next = NULL;

    return ngx_http_output_filter(r, &out);
}


/* load config file */
static char *
ngx_http_lifecycle_pass(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_int_t                       enable;
    ngx_str_t                      *value, s;
    ngx_http_core_loc_conf_t       *clcf;

    value = cf->args->elts;
    if (ngx_strncmp(value[1].data, "enable=", 7) == 0) {
        s.len = value[1].len - 7;
        s.data = value[1].data + 7;

        enable = ngx_atoi(s.data, s.len);
        if (!enable) {
            return NGX_CONF_OK;
        }
    }

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);

    clcf->handler = ngx_http_lifecycle_content_handler;

    return NGX_CONF_OK;
}



static char *
ngx_http_lifecycle_add_variable(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{

    ngx_int_t             n;
    ngx_http_variable_t  *var_on_off, *var_file_name, *var_version,
                         *var_expire_time, *var_app_key, *var_action,
                         *var_timetype;

    /* ngx_lifecycle_onoff */
    var_on_off = ngx_http_add_variable(cf, &ngx_lifecycle_onoff, 0);
    if (var_on_off == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_onoff);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_onoff_index = n;

    var_on_off->get_handler = ngx_http_lifecycle_on_off_handler;

    /* ngx_lifecycle_filename */
    var_file_name = ngx_http_add_variable(cf, &ngx_lifecycle_filename, 0);
    if (var_file_name == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_filename);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_filename_index = n;

    var_file_name->get_handler = ngx_http_lifecycle_file_name_handler;

    /* ngx_lifecycle_version */
    var_version = ngx_http_add_variable(cf, &ngx_lifecycle_version, 0);
    if (var_version == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_version);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_version_index = n;

    var_version->get_handler = ngx_http_lifecycle_version_handler;

    /* ngx_lifecycle_expiretime */
    var_expire_time = ngx_http_add_variable(cf, &ngx_lifecycle_expiretime, 0);
    if (var_expire_time == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_expiretime);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_expiretime_index = n;

    var_expire_time->get_handler = ngx_http_lifecycle_expire_time_handler;

    /* ngx_lifecycle_appkey */
    var_app_key = ngx_http_add_variable(cf, &ngx_lifecycle_appkey, 0);
    if (var_app_key == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_appkey);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_appkey_index = n;

    var_app_key->get_handler = ngx_http_lifecycle_app_key_handler;

    /* ngx_lifecycle_action */
    var_action = ngx_http_add_variable(cf, &ngx_lifecycle_action, 0);
    if (var_action == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_action);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_action_index = n;

    var_action->get_handler = ngx_http_lifecycle_action_handler;

    /* ngx_lifecycle_timetype */
    var_timetype = ngx_http_add_variable(cf, &ngx_lifecycle_timetype, 0);
    if (var_timetype == NULL) {
        return NGX_CONF_ERROR;
    }
    n = ngx_http_get_variable_index(cf, &ngx_lifecycle_timetype);
    if (n == NGX_ERROR) {
        return NGX_CONF_ERROR;
    }
    ngx_lifecycle_timetype_index = n;

    var_timetype->get_handler = ngx_http_lifecycle_timetype_handler;

    return NGX_CONF_OK;
}


static char *
ngx_http_lifecycle_kvroot_server(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_main_conf_t  *tmcf = conf;

    ngx_url_t                  u;
    ngx_str_t                 *value, *kv_root_server;

    value = cf->args->elts;
    kv_root_server = &value[1];

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.url.len = kv_root_server->len;
    u.url.data = kv_root_server->data;
    if (ngx_parse_url(cf->pool, &u) != NGX_OK) {
        if (u.err) {
            ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                "%s in lifecycle \"%V\"", u.err, &u.url);
        }

        return NGX_CONF_ERROR;
    }

    tmcf->krs_addr = u.addrs;

    return NGX_CONF_OK;
}


static char *
ngx_http_lifecycle_keepalive(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_main_conf_t  *tmcf = conf;

    ngx_int_t                       max_cached, bucket_count;
    ngx_str_t                      *value, s;
    ngx_uint_t                      i;
    ngx_http_lifecycle_connection_pool_t     *p;

    value = cf->args->elts;
    max_cached = 0;
    bucket_count = 0;

    for (i = 1; i < cf->args->nelts; i++) {
        if (ngx_strncmp(value[i].data, "max_cached=", 11) == 0) {

            s.len = value[i].len - 11;
            s.data = value[i].data + 11;

            max_cached = ngx_atoi(s.data, s.len);

            if (max_cached == NGX_ERROR || max_cached == 0) {
                goto invalid;
            }
            continue;
        }

        if (ngx_strncmp(value[i].data, "bucket_count=", 13) == 0) {

            s.len = value[i].len - 13;
            s.data = value[i].data + 13;

            bucket_count = ngx_atoi(s.data, s.len);
            if (bucket_count == NGX_ERROR || bucket_count == 0) {
                goto invalid;
            }
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    p = ngx_http_lifecycle_connection_pool_init(cf->pool, max_cached, bucket_count);
    if (p == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "connection pool init failed");
        return NGX_CONF_ERROR;
    }

    tmcf->conn_pool = p;
    return NGX_CONF_OK;

invalid:
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                       "invalid value \"%V\" in \"%V\" directive",
                       &value[i], &cmd->name);
    return NGX_CONF_ERROR;
}


static char *
ngx_http_lifecycle_net_device(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_srv_conf_t  *tscf = conf;

    ngx_int_t                       rc;
    ngx_str_t                      *value;

    value = cf->args->elts;
    rc = ngx_http_lifecycle_get_local_ip(value[1], &tscf->local_addr);
    if (rc == NGX_ERROR) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "device is invalid(%V)", &value[1]);
        return NGX_CONF_ERROR;
    }

    ngx_inet_ntop(AF_INET, &tscf->local_addr.sin_addr, tscf->local_addr_text, NGX_INET_ADDRSTRLEN);
    return NGX_CONF_OK;
}


static char *
ngx_http_lifecycle_tackle_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_srv_conf_t  *tscf = conf;
    ngx_str_t                *value;

    if (tscf->log != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;

    tscf->log = ngx_log_create(cf->cycle, &value[1]);
    if (tscf->log == NULL) {
        return NGX_CONF_ERROR;
    }

    if (cf->args->nelts == 2) {
        tscf->log->log_level = NGX_LOG_INFO;
    }

    return ngx_log_set_levels(cf, tscf->log);
}


static char *
ngx_http_lifecycle_deprecated_oper_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_srv_conf_t  *tscf = conf;
    ngx_str_t                *value;

    if (tscf->deprecated_oper_log != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;

    tscf->deprecated_oper_log = ngx_log_create(cf->cycle, &value[1]);
    if (tscf->deprecated_oper_log == NULL) {
        return NGX_CONF_ERROR;
    }

    if (cf->args->nelts == 2) {
        tscf->deprecated_oper_log->log_level = NGX_LOG_INFO;
    }

    return ngx_log_set_levels(cf, tscf->deprecated_oper_log);
}

static char *
ngx_http_lifecycle_forbidden_oper_log(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_lifecycle_srv_conf_t  *tscf = conf;
    ngx_str_t                *value;

    if (tscf->forbidden_oper_log != NULL) {
        return "is duplicate";
    }

    value = cf->args->elts;

    tscf->forbidden_oper_log = ngx_log_create(cf->cycle, &value[1]);
    if (tscf->forbidden_oper_log == NULL) {
        return NGX_CONF_ERROR;
    }

    if (cf->args->nelts == 2) {
        tscf->forbidden_oper_log->log_level = NGX_LOG_INFO;
    }

    return ngx_log_set_levels(cf, tscf->forbidden_oper_log);
}

/* main srv loc create merge */
static void *
ngx_http_lifecycle_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_lifecycle_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_lifecycle_srv_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    return conf;
}


static char *
ngx_http_lifecycle_merge_srv_conf(ngx_conf_t *cf, void *parent, void *child)
{
    return NGX_CONF_OK;
}


static void *
ngx_http_lifecycle_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_lifecycle_main_conf_t           *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_lifecycle_main_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    conf->lifecycle_connect_timeout = NGX_CONF_UNSET_MSEC;
    conf->lifecycle_send_timeout = NGX_CONF_UNSET_MSEC;
    conf->lifecycle_read_timeout = NGX_CONF_UNSET_MSEC;


    conf->buffer_size = NGX_CONF_UNSET_SIZE;
    conf->body_buffer_size = NGX_CONF_UNSET_SIZE;

    conf->conn_pool = NGX_CONF_UNSET_PTR;

    conf->krs_addr = NGX_CONF_UNSET_PTR;

    return conf;
}


static char *
ngx_http_lifecycle_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_http_lifecycle_main_conf_t *tmcf = conf;

    if (tmcf->lifecycle_connect_timeout == NGX_CONF_UNSET_MSEC) {
        tmcf->lifecycle_connect_timeout = 3000;
    }

    if (tmcf->lifecycle_send_timeout == NGX_CONF_UNSET_MSEC) {
        tmcf->lifecycle_send_timeout = 3000;
    }

    if (tmcf->lifecycle_read_timeout == NGX_CONF_UNSET_MSEC) {
        tmcf->lifecycle_read_timeout = 3000;
    }


    if (tmcf->send_lowat == NGX_CONF_UNSET_SIZE) {
        tmcf->send_lowat = 0;
    }

    if (tmcf->buffer_size == NGX_CONF_UNSET_SIZE) {
        tmcf->buffer_size = (size_t) ngx_pagesize / 2;
    }

    if (tmcf->body_buffer_size == NGX_CONF_UNSET_SIZE) {
        tmcf->body_buffer_size = (size_t) NGX_HTTP_LIFECYCLE_DEFAULT_BODY_BUFFER_SIZE;
    }

    if (tmcf->krs_addr == NGX_CONF_UNSET_PTR) {
        tmcf->krs_addr = NULL;
    }

    return NGX_CONF_OK;
}


static void *
ngx_http_lifecycle_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_lifecycle_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_lifecycle_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

    conf->ignore_client_abort = NGX_CONF_UNSET;

    conf->intercept_errors = NGX_CONF_UNSET;

    conf->update_kmt_interval_count = NGX_CONF_UNSET_UINT;

    conf->update_kmt_fail_count = NGX_CONF_UNSET_UINT;

    return conf;
}


static char *ngx_http_lifecycle_merge_loc_conf(ngx_conf_t *cf,
    void *parent, void *child)
{
    ngx_http_lifecycle_loc_conf_t *prev = parent;
    ngx_http_lifecycle_loc_conf_t *conf = child;

    ngx_conf_merge_value(conf->ignore_client_abort,
                         prev->ignore_client_abort, 0);

    ngx_conf_merge_value(conf->intercept_errors,
                         prev->intercept_errors, 0);

    ngx_conf_merge_uint_value(conf->update_kmt_interval_count,
                              prev->update_kmt_interval_count, 100);

    ngx_conf_merge_uint_value(conf->update_kmt_fail_count,
                              prev->update_kmt_fail_count, 100);

    return NGX_CONF_OK;
}


static void
ngx_http_lifecycle_read_body_handler(ngx_http_request_t *r)
{
    ngx_int_t                rc;
    ngx_http_lifecycle_t    *t;
    ngx_connection_t        *c;

    c = r->connection;

    t = ngx_http_get_module_ctx(r, ngx_http_lifecycle_module);

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, c->log, 0,
                   "http init lifecycle, client timer: %d", c->read->timer_set);

    if (c->read->timer_set) {
        ngx_del_timer(c->read);
    }

    if (ngx_event_flags & NGX_USE_CLEAR_EVENT) {

        if (!c->write->active) {
            if (ngx_add_event(c->write, NGX_WRITE_EVENT, NGX_CLEAR_EVENT)
                == NGX_ERROR)
            {
                ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
                return;
            }
        }
    }

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "=========== ngx_http_lifecycle_read_body_handler ok===");

    if (r->request_body) {
        t->send_body = r->request_body->bufs;
        if (t->send_body == NULL) {
            ngx_http_finalize_request(r, NGX_HTTP_BAD_REQUEST);
            return;
        }
    }

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "^^^^^ will into ngx_http_lifecycle_init");
    rc = ngx_http_lifecycle_init(t);

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "^^^^^ commplete  ngx_http_lifecycle_init rc is %d",rc);
    if (rc != NGX_OK) {
      ngx_log_error(NGX_LOG_INFO, t->log, 0,
                    "^^^^^ rc is %d", rc);
        switch (rc) {
        case NGX_HTTP_SPECIAL_RESPONSE ... NGX_HTTP_INTERNAL_SERVER_ERROR:
            ngx_http_finalize_request(r, rc);
            break;
        default:
            ngx_log_error(NGX_LOG_ERR, t->log, 0,
                          "ngx_http_lifecycle_init failed");
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        }
    }
}


/* ===================== rewrite phase handler ======================= */
static ngx_int_t
ngx_http_lifecycle_var_get(ngx_http_request_t *r, ngx_http_lifecycle_t *t)
{
    ngx_http_variable_value_t  *v;
    /* on off */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_onoff_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.lifecycle_on = *((int32_t *)v->data);
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "lifecycle_on: %d", t->r_ctx.lifecycle_on);
    v = NULL;

    /* file_name */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_filename_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.file_name.data = v->data;
    t->r_ctx.file_name.len = v->len;
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "file_name: %V", &t->r_ctx.file_name);
    v = NULL;

    /* version and version */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_version_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.version = *((uint8_t *)v->data);

    ngx_log_error(NGX_LOG_INFO, t->log, 0, "version: %d", t->r_ctx.version);
    v = NULL;

    /* expire_time */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_expiretime_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.absolute_time = *((int32_t *)v->data);
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "absolute_time: %d", t->r_ctx.absolute_time);
    v = NULL;

    /* app_key */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_appkey_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.appkey.data = v->data;
    t->r_ctx.appkey.len = v->len;
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "app_key: %V", &t->r_ctx.appkey);
    v = NULL;

    /* action */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_action_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.action.code = *((uint16_t *)v->data);
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "action: %d", t->r_ctx.action.code);
    v = NULL;

    /* timetype */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_timetype_index);
    if (v == NULL || v->not_found) {
        return NGX_ERROR;
    }
    t->r_ctx.expiretime_type = *((uint8_t *)v->data);

    ngx_log_error(NGX_LOG_INFO, t->log, 0, "timetype: %d", t->r_ctx.expiretime_type);
    v = NULL;

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_var_set(ngx_http_request_t *r, ngx_http_lifecycle_restful_ctx_t *ctx)
{
    ngx_http_variable_value_t  *v;
    /* on_off */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_onoff_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    *((int32_t *)v->data) = ctx->lifecycle_on;
    v->len = sizeof(int32_t);
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===lifecycle_on->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_onoff_index);
    v = NULL;

    /* file_name */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_filename_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(v->data, ctx->file_name.data, ctx->file_name.len);
    v->len = ctx->file_name.len;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===file_name->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_filename_index);
    v = NULL;

    /* version */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_version_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    *((uint8_t *)v->data) = ctx->version;
    v->len = sizeof(uint8_t);
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===version->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_version_index);
    v = NULL;

    /* expire_time */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_expiretime_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    *((int32_t *)v->data) = ctx->absolute_time;
    v->len = sizeof(int32_t);
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===expire_time->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_expiretime_index);
    v = NULL;

    /* app_key */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_appkey_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(v->data, ctx->appkey.data, ctx->appkey.len);
    v->len = ctx->appkey.len;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===file_name->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_appkey_index);
    v = NULL;

    /* action */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_action_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    *((uint16_t *)v->data) = ctx->action.code;
    v->len = sizeof(uint16_t);
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===action->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_action_index);
    v = NULL;

    /* timetype */
    v = ngx_http_get_indexed_variable(r, ngx_lifecycle_timetype_index);
    if (v == NULL) {
        return NGX_ERROR;
    }
    *((uint8_t *)v->data) = ctx->expiretime_type;
    v->len = sizeof(uint8_t);
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "===timetype->len=== %d, var_num %d",
                  v->len, ngx_lifecycle_timetype_index);
    v = NULL;

    return NGX_OK;
}


static ngx_int_t
ngx_http_lifecycle_judge_uri(ngx_http_request_t *r)
{
    /* if rc:NGX_ERROR direct into tfs mod */
    ngx_int_t             rc;
    ngx_str_t             arg_value;
    rc = NGX_ERROR;

    if (r->method == NGX_HTTP_POST) {
        if (ngx_http_arg(r, positive_expiretime_str, ngx_strlen(positive_expiretime_str),
                         &arg_value) == NGX_OK || ngx_http_arg(r, relative_expiretime_str,
                         ngx_strlen(relative_expiretime_str), &arg_value) == NGX_OK)
        {
            rc = NGX_OK;
        }

    } else if (r->method == NGX_HTTP_PUT) {
        if (ngx_http_arg(r, positive_expiretime_str, ngx_strlen(positive_expiretime_str),
                         &arg_value) == NGX_OK || ngx_http_arg(r, relative_expiretime_str,
                         ngx_strlen(relative_expiretime_str), &arg_value) == NGX_OK)
        {
            if (ngx_memcmp(r->uri.data, lifecycle_uri_prefix,
                ngx_strlen(lifecycle_uri_prefix)) == 0)
            {
                rc = NGX_OK;
            }
        }
    } else if (r->method == NGX_HTTP_GET) {
        if (ngx_memcmp(r->uri.data, lifecycle_uri_prefix,
            ngx_strlen(lifecycle_uri_prefix)) == 0)
        {
            rc = NGX_OK;
        }
    }

    return rc;
}


static void
ngx_http_lifecycle_remove_arg(ngx_http_request_t *r,
     ngx_str_t *args, ngx_str_t *the_arg)
{
    u_char                *start, *pos, *last, *end;
    ngx_int_t             new_size;
    ngx_str_t             arg_value;

    if (ngx_http_arg(r, (u_char *) the_arg->data,
                     the_arg->len, &arg_value) == NGX_OK)
    {
        end = r->args.data + r->args.len;
        start = r->args.data;
        last = arg_value.data + arg_value.len;

        pos = arg_value.data - 1 - the_arg->len;
        if (pos == start) {
            /* this arg first arg */
            if (last != end) {
                /* this arg is not last one */
                last = last + 1;
                ngx_memcpy(args->data, last, end - last);
                args->len = end - last;
            } else {
                args->len = 0;
            }

        } else if (*(pos - 1) == '&') {
            /* this arg is not first arg */
            pos = pos - 1;
            if (last != end) {
                /* this arg is not last one */
                new_size = 0;
                ngx_memcpy(args->data, start, pos - start);
                new_size += pos - start;
                ngx_memcpy(args->data + new_size, last, end - last);
                new_size += end - last;
                args->len = new_size;
            } else {
                /* this arg is last one */
                ngx_memcpy(args->data, start, pos - start);
                args->len = pos - start;
            }
        }
    }
}


static void
ngx_http_lifecycle_update_uri(ngx_http_request_t *r,
    ngx_http_lifecycle_restful_ctx_t *ctx, ngx_str_t *uri, ngx_str_t *args)
{
    u_char                ch, *start, *pos, *last, *end;
    ngx_int_t             sum, new_len;
    ngx_str_t             arg_tmp;

    if (r->method == NGX_HTTP_GET) {
        /* rewrite to filter ,don`t into tfs mod */
        uri->data = (char *)"/content_lifecycle";
        uri->len = sizeof("/content_lifecycle") - 1;
        args->data = r->args.data;
        args->len = r->args.len;

    } else if (r->method == NGX_HTTP_POST) {
        /* rewrite to tfs get filename  */
        /* POST /v1/appkey?positive_expiretime=D2013-12-12T03:05:58 */

        uri->data = r->uri.data;
        uri->len = r->uri.len;
        if (ctx->expiretime_type == NGX_HTTP_LIFECYCLE_POSITIVE_TIME) {

            arg_tmp.data = positive_expiretime_str;
            arg_tmp.len  = ngx_strlen(positive_expiretime_str);
        } else if (ctx->expiretime_type ==
                   NGX_HTTP_LIFECYCLE_RELATIVE_TIME)
        {
            arg_tmp.data = relative_expiretime_str;
            arg_tmp.len  = ngx_strlen(relative_expiretime_str);
        }

        ngx_http_lifecycle_remove_arg(r, args, &arg_tmp);

    } else if (r->method == NGX_HTTP_PUT) {
        /* rewrite to tfs to filter */
        /* PUT /lifecycle/v1/appkey/TFS_NAME */
        /* change to GET /v1/appkey/metadata/TfsFileName */
        /* PUT /lifecycle/v2/appkey/appid/uid/file/tfsname */
        /* chaneg to GET /v2/appkey/metadata/appid/uid/file/file_name */
        r->method = NGX_HTTP_GET;
        if (ngx_memcmp(r->uri.data, lifecycle_uri_prefix,
            ngx_strlen(lifecycle_uri_prefix)) == 0)
        {
             start = r->uri.data + ngx_strlen(lifecycle_uri_prefix);
        }
        sum = 0;
        new_len = 0;
        last = r->uri.data + r->uri.len;
        for (pos = start; pos < last; pos++) {
            ch = *pos;
            if (ch == '/') sum++;
            if (sum == 3) break;
        }
        /* copy /v2/appkey/ */
        ngx_memcpy(uri->data, start, pos - start + 1);
        new_len += (pos - start + 1);
        /* copy metadata/ */
        ngx_memcpy(uri->data + new_len, (char *)"metadata/", 9);
        new_len += 9;
        /* copy appid/uid/file/file_name or TfsFileName */
        ngx_memcpy(uri->data + new_len, pos + 1, last - pos);
        new_len += (last - 1 - pos);
        uri->len = new_len;

        arg_tmp.data = positive_expiretime_str;
        arg_tmp.len  = ngx_strlen(positive_expiretime_str);

        ngx_http_lifecycle_remove_arg(r, args, &arg_tmp);

    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                  "update uri is %V", uri);
}

static ngx_int_t
ngx_http_lifecycle_rewrite_handler(ngx_http_request_t *r)
{
    ngx_int_t                             rc;
    ngx_str_t                             uri, args;
    ngx_http_lifecycle_restful_ctx_t     *ctx;

    /* first rewrite if NGX_ERROR it means no lifecycle commond */
    /* second rewrite it should return NGX_ERROR */
    if (ngx_http_lifecycle_judge_uri(r) == NGX_ERROR) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "no need into rewrite");
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "URI >>>> %V", &r->uri);
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "ARG >>>> %V", &r->args);
        return NGX_DECLINED;
    }

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_lifecycle_restful_ctx_t));
    if (ctx == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "alloc ngx_http_lifecycle_restful_ctx_t failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    rc = ngx_http_lifecycle_parse(r, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    /* set r_ctx to var */
    rc = ngx_http_lifecycle_var_set(r, ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    /* update uri */

    uri.data = ngx_pcalloc(r->pool, r->uri.len);
    if (uri.data == NULL) {
        return NGX_ERROR;
    }
    uri.len = r->uri.len;

    args.data = ngx_pcalloc(r->pool, r->args.len);
    if (args.data == NULL) {
        return NGX_ERROR;
    }
    args.len = r->args.len;

    ngx_http_lifecycle_update_uri(r, ctx, &uri, &args);

    /* internal_redirect */
    return ngx_http_internal_redirect(r, &uri, &args);
}


/* ===================== access phase handler ======================= */
ngx_int_t
ngx_http_lifecycle_access_handler(ngx_http_request_t *r)
{
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "=======into access_handler====");
    /* only deal NGX_HTTP_DELETE in this phase */
    if (r->method != NGX_HTTP_DELETE) {
        return NGX_DECLINED;
    }
    ngx_int_t                             rc;
    ngx_http_lifecycle_t                 *t;

    ngx_http_lifecycle_loc_conf_t        *llcf;//loc conf
    ngx_http_lifecycle_srv_conf_t        *lscf;
    ngx_http_lifecycle_main_conf_t       *lmcf;

    t = NULL;
    t = ngx_http_get_module_ctx(r, ngx_http_lifecycle_module);
    if (t != NULL) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                      "=======ctx had alloc====");
        return NGX_DECLINED;
    }

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_lifecycle_module);
    lscf = ngx_http_get_module_srv_conf(r, ngx_http_lifecycle_module);
    lmcf = ngx_http_get_module_main_conf(r, ngx_http_lifecycle_module);

    t = ngx_pcalloc(r->pool, sizeof(ngx_http_lifecycle_t));

    if (t == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "alloc ngx_http_lifecycle_t failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    t->pool = r->pool;
    t->data = r;
    t->log = r->connection->log;
    t->loc_conf = llcf;
    t->srv_conf = lscf;
    t->main_conf = lmcf;
    t->output.tag = (ngx_buf_tag_t) &ngx_http_lifecycle_module;
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "URI >>>> %V", &r->uri);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0,
                  "ARG >>>> %V", &r->args);

    rc = ngx_http_lifecycle_parse(r, &t->r_ctx);
    if (rc != NGX_OK) {
        return rc;
    }

    ngx_http_set_ctx(r, t, ngx_http_lifecycle_module);
    r->main->count++;
    ngx_http_lifecycle_read_body_handler(r);

    r->write_event_handler = ngx_http_lifecycle_dummy_handler;
    return NGX_AGAIN;
}


/* ====================== filter phase ==============================*/

static ngx_int_t
ngx_http_lifecycle_parse_from_tfs_output(ngx_http_lifecycle_t *t, ngx_chain_t *in)
{
    ngx_str_t                         tmp, time_tmp;
    u_char                           *p;
    size_t                            size, rest;
    ngx_buf_t                        *b;
    ngx_chain_t                      *cl;
    yajl_val                          node, val;

    ngx_log_error(NGX_LOG_INFO, t->log, 0,
                  "=======into read from_tfs_output======");
    if (t->start_output == NULL) {
        t->start_output = ngx_palloc(t->pool, NGX_HTTP_LIFECYCLE_MAX_FRAGMENT_SIZE);
        if (t->start_output == NULL) {
            return NGX_ERROR;
        }

        t->last_output = t->start_output;
    }

    p = t->last_output;

    for (cl = in; cl; cl = cl->next) {

        b = cl->buf;
        size = b->last - b->pos;

        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, t->log, 0,
                       "filter buf: %uz", size);
        tmp.data = b->pos;
        tmp.len = size;
        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                            "is this buf %V", &tmp);
        rest = t->start_output + NGX_HTTP_LIFECYCLE_MAX_FRAGMENT_SIZE - p;
        size = (rest < size) ? rest : size;

        p = ngx_cpymem(p, b->pos, size);
        b->pos += size;

        if (b->last_buf) {
            t->last_output = p;
            ngx_log_error(NGX_LOG_INFO, t->log, 0,
                            "is last buf");
            tmp.data = t->start_output;
            tmp.len = t->last_output - t->start_output;
            ngx_log_error(NGX_LOG_INFO, t->log, 0,
                          "is all buf %V", &tmp);

            switch (t->r_ctx.action.code) {

                case NGX_HTTP_LIFECYCLE_ACTION_POST:
                    node = yajl_tree_parse((const char *) t->start_output,
                                            NULL, 0);
                    if (node == NULL) {
                        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "PARSE ERROR, node is null");
                        return NGX_ERROR;
                    }
                    const char * path[] = {"TFS_FILE_NAME", NULL};
                    val = yajl_tree_get(node, path, yajl_t_string);
                    if (val) {

                        t->r_ctx.file_name.data = ngx_pcalloc(t->pool,
                             sizeof(NGX_HTTP_LIFECYCLE_MAX_FILE_NAME_LEN));
                        if (t->r_ctx.file_name.data == NULL) {
                            ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                          "alloc FILE_NAME failed");
                            return NGX_ERROR;
                        }
                        ngx_memcpy(t->r_ctx.file_name.data, YAJL_GET_STRING(val),
                                   ngx_strlen(YAJL_GET_STRING(val)));

                        t->r_ctx.file_name.len = ngx_strlen(YAJL_GET_STRING(val));
                        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "file_name is %V", &t->r_ctx.file_name);
                    } else {
                        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "PARSE ERROR, no TFS_FILE_NAME");
                        return NGX_ERROR;
                    }

                    yajl_tree_free(node);

                    break;
                case NGX_HTTP_LIFECYCLE_ACTION_PUT:

                    node = yajl_tree_parse((const char *) t->start_output,
                                            NULL, 0);
                    if (node == NULL) {
                        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "PARSE ERROR, node is null");
                        return NGX_ERROR;
                    }
                    const char * path1[] = {"CREATE_TIME", NULL};
                    val = yajl_tree_get(node, path1, yajl_t_string);
                    time_tmp.data = YAJL_GET_STRING(val);
                    time_tmp.len = ngx_strlen(YAJL_GET_STRING(val));
                    if (val) {
                        if (t->r_ctx.expiretime_type == NGX_HTTP_LIFECYCLE_RELATIVE_TIME) {
                            if (t->r_ctx.absolute_time != 0) {
                                ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "PUT relative_time is %d", t->r_ctx.absolute_time);
                                t->r_ctx.absolute_time += ngx_http_lifecycle_get_mk_time(&time_tmp);
                                ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                              "PUT absolute_time is %d", t->r_ctx.absolute_time);
                            } else {
                               return NGX_ERROR;
                            }
                        }
                    } else {
                        ngx_log_error(NGX_LOG_INFO, t->log, 0,
                                      "PARSE ERROR, no CREATE_TIME");
                        return NGX_ERROR;
                    }

                    yajl_tree_free(node);
                    break;

                case NGX_HTTP_LIFECYCLE_ACTION_DEL:
                    break;
                default:
                    return;
            }

            return NGX_OK;
        }
    }
    t->last_output = p;
    return NGX_AGAIN;
}


ngx_int_t
ngx_http_lifecycle_send_body(ngx_http_lifecycle_t *t)
{
    ngx_buf_t                       *new_buf;
    ngx_chain_t                     *out;
    ngx_http_request_t              *r;

    r = (ngx_http_request_t *)(t->data);
    t->json_output = ngx_http_lifecycle_json_init(t->log, t->pool);

    if (t->lifecycle_status != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
              "=====into json return errorno=========");
        out = ngx_http_lifecycle_json_errorno(t->json_output,
                                             t->lifecycle_status);
        out->next = NULL;
    } else {

        switch (t->r_ctx.action.code) {

            case NGX_HTTP_LIFECYCLE_ACTION_POST:
            case NGX_HTTP_LIFECYCLE_ACTION_PUT:
            case NGX_HTTP_LIFECYCLE_ACTION_GET:
                out = ngx_http_lifecycle_json_lifecycle_info(t->json_output,
                    &t->r_ctx.file_name, t->r_ctx.absolute_time);
                break;
            default:
                return;
        }
        out->next = NULL;
    }

    ngx_http_next_header_filter(r);
    return ngx_http_next_body_filter(r, out);
}


static ngx_int_t
ngx_http_lifecycle_header_filter(ngx_http_request_t *r)
{
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "=====http lifecycle header filter");
    if (r->method == NGX_HTTP_DELETE) {
        return ngx_http_next_header_filter(r);
    }

    ngx_int_t                             rc;
    ngx_http_lifecycle_t                 *t;
    ngx_http_lifecycle_loc_conf_t        *llcf;//loc conf
    ngx_http_lifecycle_srv_conf_t        *lscf;
    ngx_http_lifecycle_main_conf_t       *lmcf;

    llcf = ngx_http_get_module_loc_conf(r, ngx_http_lifecycle_module);
    lscf = ngx_http_get_module_srv_conf(r, ngx_http_lifecycle_module);
    lmcf = ngx_http_get_module_main_conf(r, ngx_http_lifecycle_module);

    t = ngx_pcalloc(r->pool, sizeof(ngx_http_lifecycle_t));

    if (t == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "alloc ngx_http_lifecycle_t failed");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    t->pool = r->pool;
    t->data = r;
    t->log = r->connection->log;
    t->loc_conf = llcf;
    t->srv_conf = lscf;
    t->main_conf = lmcf;
    t->output.tag = (ngx_buf_tag_t) &ngx_http_lifecycle_module;

    /* get var to r_ctx */
    rc = ngx_http_lifecycle_var_get(r, t);
    if (rc != NGX_OK) {
        return rc;
    }
    ngx_http_set_ctx(r, t, ngx_http_lifecycle_module);
    if (0 == t->r_ctx.lifecycle_on) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "lifecycle is off");
        return ngx_http_next_header_filter(r);
    }

    r->main_filter_need_in_memory = 1;
    r->allow_ranges = 0;
    //return ngx_http_next_header_filter(r);
    return NGX_OK;
}



static ngx_int_t
ngx_http_lifecycle_body_filter(ngx_http_request_t *r, ngx_chain_t *in)
{

    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "=====http lifecycle body filter");
    u_char                           *p;
    size_t                            size, rest;
    ngx_int_t                         rc;
    ngx_buf_t                        *b, *new_buf;
    ngx_chain_t                      *cl, out;
    ngx_http_lifecycle_t             *t;

    if (in == NULL || r->method == NGX_HTTP_DELETE) {
        return ngx_http_next_body_filter(r, in);
    }

    t = ngx_http_get_module_ctx(r, ngx_http_lifecycle_module);

    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "lifecycle is ====%d", t->r_ctx.lifecycle_on);

    if (0 == t->r_ctx.lifecycle_on) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "lifecycle is off");
        return ngx_http_next_body_filter(r, in);
    }

    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "now uri is %V", &r->uri);
    ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "now method is %d", r->method);

    rc = ngx_http_lifecycle_parse_from_tfs_output(t, in);
    if (NGX_AGAIN == rc) {
        /* is not last buf */
        return NGX_OK;
    } else if (NGX_ERROR == rc) {
        ngx_log_error(NGX_LOG_INFO, r->connection->log, 0, "TFS RETURN ERROR");
        return ngx_http_next_body_filter(r, in);
    }

    r->main->count++;
    ngx_http_lifecycle_read_body_handler(r);

    return NGX_OK;

}


static ngx_int_t
ngx_http_lifecycle_filter_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *rewrite_h, *access_h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    rewrite_h = ngx_array_push(&cmcf->phases[NGX_HTTP_REWRITE_PHASE].handlers);
    if (rewrite_h == NULL) {
            return NGX_ERROR;
    }
    *rewrite_h = ngx_http_lifecycle_rewrite_handler;

    access_h = ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
    if (access_h == NULL) {
            return NGX_ERROR;
    }
    *access_h = ngx_http_lifecycle_access_handler;

    ngx_http_next_body_filter = ngx_http_top_body_filter;
    ngx_http_top_body_filter = ngx_http_lifecycle_body_filter;

    ngx_http_next_header_filter = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_lifecycle_header_filter;

    return NGX_OK;
}
