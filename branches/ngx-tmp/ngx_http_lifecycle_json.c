
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_lifecycle_common.h>
#include <ngx_http_lifecycle_json.h>


ngx_http_lifecycle_json_gen_t *
ngx_http_lifecycle_json_init(ngx_log_t *log, ngx_pool_t *pool)
{
    yajl_gen                                g;
    ngx_http_lifecycle_json_gen_t          *tj_gen;

    g = yajl_gen_alloc(NULL);
    if (g == NULL) {
        ngx_log_error(NGX_LOG_ERR, log, errno, "alloc yajl_gen failed");
        return NULL;
    }

    tj_gen = ngx_pcalloc(pool, sizeof(ngx_http_lifecycle_json_gen_t));
    if (tj_gen == NULL) {
        return NULL;
    }

    yajl_gen_config(g, yajl_gen_beautify, 1);

    tj_gen->gen = g;
    tj_gen->pool = pool;
    tj_gen->log = log;

    return tj_gen;
}


void
ngx_http_lifecycle_json_destroy(ngx_http_lifecycle_json_gen_t *tj_gen)
{
    if (tj_gen != NULL) {
        yajl_gen_free(tj_gen->gen);
    }
}


ngx_chain_t *
ngx_http_lifecycle_json_lifecycle_info(ngx_http_lifecycle_json_gen_t *tj_gen,
    ngx_str_t *file_name, ngx_int_t expire_time)
{
    size_t                      size;
    u_char                      time_buf[NGX_HTTP_LIFECYCLE_GMT_TIME_SIZE];
    yajl_gen                    g;
    ngx_buf_t                  *b;
    ngx_chain_t                *cl;

    g = tj_gen->gen;
    size = 0;

    yajl_gen_map_open(g);
    yajl_gen_string(g, (const unsigned char *) "LIFECYCLE_FILE_NAME", 19);
    yajl_gen_string(g, (const unsigned char *) file_name->data, file_name->len);

    ngx_http_lifecycle_time(time_buf, (uint32_t)expire_time);
    yajl_gen_string(g, (const unsigned char *) "EXPIRE_TIME", 11);
    yajl_gen_string(g, time_buf, NGX_HTTP_LIFECYCLE_GMT_TIME_SIZE);
    yajl_gen_map_close(g);

    cl = ngx_alloc_chain_link(tj_gen->pool);
    if (cl == NULL) {
        return NULL;
    }
    cl->next = NULL;

    b = ngx_calloc_buf(tj_gen->pool);
    if (b == NULL) {
        return NULL;
    }
    yajl_gen_get_buf(g, (const unsigned char **) &b->pos, &size);
    b->last = b->pos + size;
    b->end = b->last;
    b->temporary = 1;
    b->flush = 1;
    b->last_buf = 1;
    cl->buf = b;
    return cl;
}


ngx_chain_t *
ngx_http_lifecycle_json_errorno(ngx_http_lifecycle_json_gen_t *tj_gen, int32_t error_no)
{
    size_t                      size;
    yajl_gen                    g;
    ngx_buf_t                  *b;
    ngx_chain_t                *cl;

    g = tj_gen->gen;
    size = 0;

    yajl_gen_map_open(g);
    yajl_gen_string(g, (const unsigned char *) "LIFECYCLE_ERROR_NUM", 19);
    yajl_gen_integer(g, error_no);
    yajl_gen_map_close(g);

    cl = ngx_alloc_chain_link(tj_gen->pool);
    if (cl == NULL) {
        return NULL;
    }
    cl->next = NULL;

    b = ngx_calloc_buf(tj_gen->pool);
    if (b == NULL) {
        return NULL;
    }
    yajl_gen_get_buf(g, (const unsigned char **) &b->pos, &size);
    b->last = b->pos + size;
    b->end = b->last;
    b->temporary = 1;
    b->flush = 1;
    b->last_buf = 1;
    cl->buf = b;
    return cl;
}
