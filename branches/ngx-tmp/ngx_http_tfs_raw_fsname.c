
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: mingyan
 * Email: mingyan.zc@taobao.com
 */


#include <ngx_http_tfs_raw_fsname.h>


static const char enc_table[] = "XabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWYZ0123456789_.";
static const char dec_table[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  \
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,63,0,52,53,54,55,  \
    56,57,58,59,60,61,0,0,0,0,0,0,0,27,28,29,30,31,32,33,34,35,36,37,38,39,\
    40,41,42,43,44,45,46,47,48,49,0,50,51,0,0,0,0,62,0,1,2,3,4,5,6,7,8,9,  \
    10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,0,0,0,0,0,0,0,0,0,  \
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, \
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, \
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, \
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};


ngx_int_t
ngx_http_tfs_raw_fsname_parse(ngx_str_t *tfs_name, ngx_str_t *suffix, ngx_http_tfs_raw_fsname_t* fsname)
{
    ngx_uint_t            suffix_len;

    if (fsname != NULL && tfs_name->data != NULL && tfs_name->data[0] != '\0') {
        ngx_memzero(fsname, sizeof(ngx_http_tfs_raw_fsname_t));
        fsname->file_type = ngx_http_tfs_raw_fsname_check_file_type(tfs_name);
        if (fsname->file_type == NGX_HTTP_TFS_INVALID_FILE_TYPE) {
            return NGX_ERROR;
        } else {
            /* if two suffix exist, check consistency */
            if (suffix != NULL && suffix->data != NULL && tfs_name->len > NGX_HTTP_TFS_FILE_NAME_LEN) {
                suffix_len = tfs_name->len - NGX_HTTP_TFS_FILE_NAME_LEN;
                if (suffix->len != suffix_len) {
                    return NGX_ERROR;
                }
                suffix_len = suffix->len > suffix_len ? suffix_len : suffix->len;
                if (ngx_memcmp(suffix->data, tfs_name->data + NGX_HTTP_TFS_FILE_NAME_LEN, suffix_len)) {
                    return NGX_ERROR;
                }
            }

            ngx_http_tfs_raw_fsname_decode(tfs_name->data + 2, (u_char*) &(fsname->file));
            if (suffix != NULL && suffix->data == NULL) {
                suffix->data = tfs_name->data + NGX_HTTP_TFS_FILE_NAME_LEN;
                suffix->len = tfs_name->len - NGX_HTTP_TFS_FILE_NAME_LEN;
            }

            ngx_http_tfs_raw_fsname_set_suffix(fsname, suffix);
            if (fsname->cluster_id == 0) {
                fsname->cluster_id = tfs_name->data[1] - '0';
            }
        }
    }

    return NGX_OK;
}

u_char*
ngx_http_tfs_raw_fsname_get_name(ngx_http_tfs_raw_fsname_t* fsname,
    unsigned large_flag, ngx_int_t simple_name)
{
    if (fsname != NULL) {
        if (simple_name) {  // zero suffix
            fsname->file.suffix = 0;
        }

        ngx_http_tfs_raw_fsname_encode((u_char*) &(fsname->file), fsname->file_name + 2);

        if (large_flag) {
            fsname->file_name[0] = NGX_HTTP_TFS_LARGE_FILE_KEY_CHAR;

        } else {
            fsname->file_name[0] = NGX_HTTP_TFS_SMALL_FILE_KEY_CHAR;
        }
        fsname->file_name[1] = (u_char) ('0' + fsname->cluster_id);
        fsname->file_name[NGX_HTTP_TFS_FILE_NAME_LEN] = '\0';

        return fsname->file_name;
    }

    return NULL;
}


ngx_http_tfs_raw_file_type_e
ngx_http_tfs_raw_fsname_check_file_type(ngx_str_t *tfs_name)
{
    ngx_http_tfs_raw_file_type_e     file_type = NGX_HTTP_TFS_INVALID_FILE_TYPE;

    if (tfs_name->data != NULL
        && tfs_name->len >= NGX_HTTP_TFS_FILE_NAME_LEN)
    {
        if (tfs_name->data[0] == NGX_HTTP_TFS_LARGE_FILE_KEY_CHAR) {
            file_type = NGX_HTTP_TFS_LARGE_FILE_TYPE;

        } else if (tfs_name->data[0] == NGX_HTTP_TFS_SMALL_FILE_KEY_CHAR) {
            file_type = NGX_HTTP_TFS_SMALL_FILE_TYPE;
        }
    }

    return file_type;
}


void
ngx_http_tfs_raw_fsname_encode(u_char *input, u_char *output)
{
    uint32_t      value;
    ngx_uint_t    i, k;

    i = 0;
    k = 0;
    value = 0;

    if (input != NULL && output != NULL) {
        for (i = 0; i < NGX_HTTP_TFS_FILE_NAME_EXCEPT_SUFFIX_LEN; i += 3) {
            value = ((input[i] << 16) & 0xff0000) + ((input[i + 1] << 8) & 0xff00) + (input[i + 2] & 0xff);
            output[k++] = enc_table[value >> 18];
            output[k++] = enc_table[(value >> 12) & 0x3f];
            output[k++] = enc_table[(value >> 6) & 0x3f];
            output[k++] = enc_table[value & 0x3f];
        }
    }
}


void
ngx_http_tfs_raw_fsname_decode(u_char *input, u_char *output)
{
    uint32_t       value = 0;
    ngx_uint_t     i, k;

    i = 0;
    k = 0;
    value = 0;

    if (input != NULL && output != NULL) {
        for (i = 0; i < NGX_HTTP_TFS_FILE_NAME_LEN - 2; i += 4) {
            value = (dec_table[input[i] & 0xff] << 18) + (dec_table[input[i + 1] & 0xff] << 12) +
                (dec_table[input[i + 2] & 0xff] << 6) + dec_table[input[i + 3] & 0xff];
            output[k++] = (u_char) ((value >> 16) & 0xff);
            output[k++] = (u_char) ((value >> 8) & 0xff);
            output[k++] = (u_char) (value & 0xff);
        }
    }
}
