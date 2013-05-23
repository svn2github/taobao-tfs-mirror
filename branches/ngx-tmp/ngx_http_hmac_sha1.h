#include <stdio.h>
#include <string.h>
#include <stdlib.h>
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
static int hmac_sha1(const unsigned char* secret, unsigned int secret_len,
                    unsigned char* str_to_sign, unsigned int str_to_sign_len,
                    unsigned char* signature, unsigned int *signature_len)
{
    unsigned int md_len;
    unsigned char md[EVP_MAX_MD_SIZE];

    if (evp_md==NULL)
    {
       evp_md = EVP_sha1();
    }


    HMAC(evp_md, secret, secret_len, str_to_sign, str_to_sign_len, md, &md_len);
    /*
    unsigned char *dig = (unsigned char*)malloc(100*sizeof(unsigned char));
    sprintf((char*)dig, "AWS:%s", md);
    printf("mdlen is:%d\n", md_len);
    for(int i=0;i<md_len;i++)
    {
      printf("%02x",dig[i]);
    }
    printf("\n");
    free(dig);
    dig = NULL;
    */
    BIO* b64 = BIO_new(BIO_f_base64());
    BIO* bmem = BIO_new(BIO_s_mem());
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, md, md_len);
    (void)BIO_flush(b64);
    BUF_MEM *bptr;
    BIO_get_mem_ptr(b64, &bptr);

    memcpy(str_to_sign, bptr->data, bptr->length-1);
    str_to_sign[bptr->length-1]='\0';
    *signature_len = bptr->length-1;
    BIO_free_all(b64);

    sprintf((char*)signature, "TFS:%s", str_to_sign);
    *signature_len += 4;

    /*
    printf("signature is :%s\n", signature);
    */

    return 0;
}
/*
int main()
{
  char secret[MAX_SECRET_SIZE];
  char str_to_sign[MAX_STR_TO_SIGN_SIZE];
  char signature[MAX_SIGNATURE_SIZE];

  strcpy(str_to_sign, "GET");
  strcpy(str_to_sign + 3, "\n");
  strcpy(str_to_sign + 4, "\n");
  strcpy(str_to_sign + 5, "\n");
  strcpy(str_to_sign + 6, "Tue, 27 Mar 2007 19:36:42 +0000");
  strcpy(str_to_sign + 37, "\n");
  strcpy(str_to_sign + 38, "/johnsmith/photos/puppy.jpg");
  unsigned int str_to_sign_len = 65;
  strcpy(secret, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
  unsigned int secret_len = 40;

  hmac_sha1((unsigned char*)secret, secret_len, (unsigned char*)str_to_sign, str_to_sign_len, signature);
  return 0;
}
*/
