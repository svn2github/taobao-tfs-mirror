
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */



#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ngx_http_hmac_sha1.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

static const EVP_MD* evp_md = NULL;
/*
unsigned int MAX_SECRET_SIZE = 256;
unsigned int MAX_STR_TO_SIGN_SIZE = 1024;
unsigned int MAX_SIGNATURE_SIZE = 1024;
*/
int hmac_sha1(const u_char* secret, ngx_uint_t secret_len,
    const u_char* str_to_sign, ngx_uint_t str_to_sign_len,
    u_char* signature, ngx_uint_t max_signature_size, ngx_uint_t *signature_len)
{
    unsigned int md_len;
    u_char md[EVP_MAX_MD_SIZE];

    if (evp_md==NULL)
    {
       evp_md = EVP_sha1();
    }


    HMAC(evp_md, secret, secret_len, str_to_sign, str_to_sign_len, md, &md_len);

    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* bmem = BIO_new(BIO_s_mem());
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, md, md_len);
    (void)BIO_flush(b64);
    BUF_MEM *bptr;
    BIO_get_mem_ptr(b64, &bptr);

    if (max_signature_size < bptr->length-1) {
        return -1;
    }

    memcpy(signature, bptr->data, bptr->length-1);
    signature[bptr->length-1]='\0';
    *signature_len = bptr->length-1;
    BIO_free_all(b64);

    return 0;
}

