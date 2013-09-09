
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#include <ngx_http_lifecycle_serialization.h>


ngx_int_t
ngx_http_lifecycle_serialize_string(u_char **p,
    ngx_str_t *string)
{
    if (p == NULL || *p == NULL || string == NULL) {
        return NGX_ERROR;
    }

    if (string->len == 0) {
        *((uint32_t *)*p) = 0;

    } else {
        *((uint32_t *)*p) = string->len + 1;
    }
    *p += sizeof(uint32_t);

    if (string->len > 0) {
        ngx_memcpy(*p, string->data, string->len);
        *p += string->len + 1;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_deserialize_string(u_char **p, ngx_pool_t *pool,
    ngx_str_t *string)
{
    if (p == NULL || *p == NULL || pool == NULL || string == NULL) {
        return NGX_ERROR;
    }

    string->len = *((uint32_t *)*p);
    (*p) += sizeof(uint32_t);

    if (string->len > 0) {
        /* this length includes '/0' */
        string->len -= 1;
        string->data = ngx_pcalloc(pool, string->len);
        if (string->data == NULL) {
            return NGX_ERROR;
        }
        ngx_memcpy(string->data, (*p), string->len);
        (*p) += string->len + 1;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_deserialize_vstring(u_char **p, ngx_pool_t *pool,
    uint32_t *count, ngx_str_t **string)
{
    uint32_t   new_count, i;
    ngx_int_t  rc;

    if (p == NULL || *p == NULL || pool == NULL
        || count == NULL || string == NULL)
    {
        return NGX_ERROR;
    }

    /* count */
    new_count = *((uint32_t *)*p);
    (*p) += sizeof(uint32_t);

    /* string */
    if (new_count > 0) {
        if (*string == NULL) {
            *string = ngx_pcalloc(pool, sizeof(ngx_str_t) * new_count);
            if (*string == NULL) {
                return NGX_ERROR;
            }

        } else if (new_count > *count) {
            *string = ngx_prealloc(pool, *string, sizeof(ngx_str_t) * (*count),
                                   sizeof(ngx_str_t) * new_count);
            if (*string == NULL) {
                return NGX_ERROR;
            }
            ngx_memzero(*string, sizeof(ngx_str_t) * new_count);
        }
        for (i = 0; i < new_count; i++) {
            rc = ngx_http_lifecycle_deserialize_string(p, pool, (*string) + i);
            if (rc == NGX_ERROR) {
                return NGX_ERROR;
            }
        }
    }
    *count = new_count;

    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_serialize_vuint64(u_char **p, uint32_t count,
    uint64_t *elts)
{
    uint32_t  i;

    if (p == NULL || *p == NULL || elts == NULL) {
        return NGX_ERROR;
    }

    /* count */
    *((uint32_t *) *p) = count;
    (*p) += sizeof(uint32_t);

    /* elts */
    for (i = 0; i < count; i++) {
        *((uint64_t *) *p) = elts[i];
        (*p) += sizeof(uint64_t);
    }

    return NGX_OK;
}



ngx_int_t
ngx_http_lifecycle_deserialize_kv_meta_table(u_char **p,
    ngx_http_lifecycle_kv_meta_table_t *kv_meta_table)
{
    uint32_t   type_tag, table_size, i;
    ngx_int_t  rc;

    if (p == NULL || *p == NULL || kv_meta_table == NULL) {
        return NGX_ERROR;
    }

    rc = NGX_OK;
    while (rc == NGX_OK) {
        type_tag = *((uint32_t *)*p);
        (*p) += sizeof(uint32_t);

        switch (type_tag) {
        case NGX_HTTP_LIFECYCLE_KV_META_TABLE_V_META_TABLE_TAG:
            table_size = *((uint32_t *)*p);
            (*p) += sizeof(uint32_t);
            if (table_size == 0) {
                return NGX_ERROR;
            }

            for (i = 0; i < table_size; i++) {
                *(uint64_t *)(&kv_meta_table->table[i]) = *((uint64_t *)*p);
                (*p) += sizeof(uint64_t);
            }
            kv_meta_table->size = table_size;
            break;
        case NGX_HTTP_LIFECYCLE_END_TAG:
            break;
        default:
            rc = NGX_ERROR;
            break;
        }

        if (type_tag == NGX_HTTP_LIFECYCLE_END_TAG) {
            break;
        }
    }

    return rc;
}
