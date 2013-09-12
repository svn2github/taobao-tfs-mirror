
/*
 * taobao lifecycle for nginx
 *
 * This module is designed to support restful interface to lifecycle
 *
 * Author: qixiao.zs
 * Email: qixiao.zs@alibaba-inc.com
 */


#include <ngx_lifecycle_common.h>
#include <ngx_http_lifecycle_protocol.h>
#include <ngx_http_lifecycle_errno.h>
#include <net/if.h>
#include <ngx_md5.h>
#include <ngx_http_lifecycle_peer_connection.h>


static char  *week[] = { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
static char  *months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

/* global variable */
char *positive_expiretime_str = "positive_expiretime";
char *relative_expiretime_str = "relative_expiretime";
char *lifecycle_uri_prefix = "/lifecycle";



ngx_int_t
ngx_http_lifecycle_test_connect(ngx_connection_t *c)
{
    int        err;
    socklen_t  len;

#if (NGX_HAVE_KQUEUE)

    if (ngx_event_flags & NGX_USE_KQUEUE_EVENT)  {
        if (c->write->pending_eof) {
            c->log->action = "connecting to upstream";
            (void) ngx_connection_error(c, c->write->kq_errno,
                "kevent() reported that connect() failed");
            return NGX_ERROR;
        }

    } else
#endif
    {
        err = 0;
        len = sizeof(int);

        /*
         * BSDs and Linux return 0 and set a pending error in err
         * Solaris returns -1 and sets errno
         */

        if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (void *) &err, &len)
            == -1)
        {
            err = ngx_errno;
        }

        if (err) {
            c->log->action = "connecting to upstream";
            (void) ngx_connection_error(c, err, "connect() failed");
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


uint64_t
ngx_http_lifecycle_generate_packet_id(void)
{
    static uint64_t id = 2;

    if (id >= INT_MAX - 1) {
        id = 1;
    }

    return ++id;
}


ngx_chain_t *
ngx_http_lifecycle_alloc_chains(ngx_pool_t *pool, size_t count)
{
    ngx_uint_t               i;
    ngx_chain_t             *cl, **ll;

    ll = &cl;

    for (i = 0; i < count; i++) {
        *ll = ngx_alloc_chain_link(pool);
        if (*ll == NULL) {
            return NULL;
        }

        ll = &(*ll)->next;
    }

    (*ll) = NULL;

    return cl;
}


ngx_chain_t *
ngx_http_lifecycle_chain_get_free_buf(ngx_pool_t *p,
    ngx_chain_t **free, size_t size)
{
    ngx_chain_t  *cl;

    if (*free) {
        cl = *free;
        if ((size_t) (cl->buf->end - cl->buf->start) >= size) {
            *free = cl->next;
            cl->next = NULL;
            return cl;
        }
    }

    cl = ngx_alloc_chain_link(p);
    if (cl == NULL) {
        return NULL;
    }

    cl->buf = ngx_create_temp_buf(p, size);
    if (cl->buf == NULL) {
        return NULL;
    }

    cl->next = NULL;

    return cl;
}


void
ngx_http_lifecycle_free_chains(ngx_chain_t **free, ngx_chain_t **out)
{
    ngx_chain_t              *cl;

    cl = *out;

    while(cl) {
        cl->buf->pos = cl->buf->start;
        cl->buf->last = cl->buf->start;
        cl->buf->file_pos = 0;

        cl->next = *free;
        *free = cl;
    }
}


ngx_int_t
ngx_http_lifecycle_parse_headerin(ngx_http_request_t *r, ngx_str_t *header_name,
    ngx_str_t *value)
{
    ngx_uint_t        i;
    ngx_list_part_t  *part;
    ngx_table_elt_t  *header;

    part = &r->headers_in.headers.part;
    header = part->elts;

    for (i = 0; /* void */; i++) {

        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }

            part = part->next;
            header = part->elts;
            i = 0;
        }

        if (header[i].hash == 0) {
            continue;
        }

        if (header_name->len ==  header[i].key.len
            && ngx_strncasecmp(header[i].key.data, header_name->data,
                               header_name->len) == 0)
        {
            *value = header[i].value;
            return NGX_OK;
        }
    }

    return NGX_DECLINED;
}


ngx_int_t
ngx_http_lifecycle_compute_buf_crc(ngx_http_lifecycle_crc_t *t_crc, ngx_buf_t *b,
    size_t size, ngx_log_t *log)
{
    u_char              *dst;
    ssize_t              n;

    if (ngx_buf_in_memory(b)) {
        t_crc->crc = ngx_http_lifecycle_crc(t_crc->crc, (const char *) (b->pos), size);
        t_crc->data_crc = ngx_http_lifecycle_crc(t_crc->data_crc, (const char *) (b->pos), size);
        b->last = b->pos + size;
        return NGX_OK;
    }

    dst = ngx_alloc(size, log);
    if (dst == NULL) {
        return 0;
    }

    n = ngx_read_file(b->file, dst, (size_t) size, b->file_pos);

    if (n == NGX_ERROR) {
        goto crc_error;
    }

    if (n != (ssize_t) size) {
        ngx_log_error(NGX_LOG_ALERT, log, 0,
                      ngx_read_file_n " read only %z of %O from \"%s\"",
                      n, size, b->file->name.data);
        goto crc_error;
    }

    t_crc->crc = ngx_http_lifecycle_crc(t_crc->crc, (const char *) dst, size);
    t_crc->data_crc = ngx_http_lifecycle_crc(t_crc->data_crc, (const char *) dst, size);
    free(dst);

    b->file_last = b->file_pos + n;
    return NGX_OK;

crc_error:
    free(dst);
    return NGX_ERROR;
}


ngx_int_t
ngx_http_lifecycle_peer_set_addr(ngx_pool_t *pool, ngx_http_lifecycle_peer_connection_t *p,
    ngx_http_lifecycle_inet_t *addr)
{
    struct sockaddr_in     *in;
    ngx_peer_connection_t  *peer;

    if (addr == NULL) {
        return NGX_ERROR;
    }

    in = ngx_pcalloc(pool, sizeof(struct sockaddr_in));
    if (in == NULL) {
        return NGX_ERROR;
    }

    in->sin_family = AF_INET;
    in->sin_port = htons(addr->port);
    in->sin_addr.s_addr = addr->ip;

    peer = &p->peer;
    peer->sockaddr = (struct sockaddr *) in;
    peer->socklen = sizeof(struct sockaddr_in);

    ngx_sprintf(p->peer_addr_text, "%s:%d",
                inet_ntoa(in->sin_addr),
                ntohs(in->sin_port));

    return NGX_OK;
}


u_char*
ngx_http_lifecycle_get_peer_addr_text(ngx_http_lifecycle_inet_t *addr)
{
    static u_char peer_addr_text[30];
    struct sockaddr_in  in;

    if (addr == NULL) {
        return NULL;
    }

    in.sin_family = AF_INET;
    in.sin_port = htons(addr->port);
    in.sin_addr.s_addr = addr->ip;

    ngx_sprintf(peer_addr_text, "%s:%d",
                inet_ntoa(in.sin_addr),
                ntohs(in.sin_port));

    return peer_addr_text;
}


uint32_t
ngx_http_lifecycle_murmur_hash(u_char *data, size_t len)
{
    uint32_t  h, k;

    h = NGX_HTTP_LIFECYCLE_MUR_HASH_SEED ^ len;

    while (len >= 4) {
        k  = data[0];
        k |= data[1] << 8;
        k |= data[2] << 16;
        k |= data[3] << 24;

        k *= 0x5bd1e995;
        k ^= k >> 24;
        k *= 0x5bd1e995;

        h *= 0x5bd1e995;
        h ^= k;

        data += 4;
        len -= 4;
    }

    switch (len) {
    case 3:
        h ^= data[2] << 16;
    case 2:
        h ^= data[1] << 8;
    case 1:
        h ^= data[0];
        h *= 0x5bd1e995;
    }

    h ^= h >> 13;
    h *= 0x5bd1e995;
    h ^= h >> 15;

    return h;
}


ngx_int_t
ngx_http_lifecycle_parse_inet(ngx_str_t *u, ngx_http_lifecycle_inet_t *addr)
{
    u_char              *port, *last;
    size_t               len;
    ngx_int_t            n;

    last = u->data + u->len;

    port = ngx_strlchr(u->data, last, ':');

    if (port) {
        port++;

        len = last - port;

        if (len == 0) {
            return NGX_ERROR;
        }

        n = ngx_atoi(port, len);

        if (n < 1 || n > 65535) {
            return NGX_ERROR;
        }

        addr->port = n;

        addr->ip = ngx_inet_addr(u->data, u->len - len - 1);
        if (addr->ip == INADDR_NONE) {
            return NGX_ERROR;
        }

    } else {
        return NGX_ERROR;
    }

    return NGX_OK;
}


int32_t
ngx_http_lifecycle_raw_fsname_hash(const u_char *str, const int32_t len)
{
    int32_t h, i;

    h = 0;
    i = 0;

    if (str == NULL || len <=0) {
        return 0;
    }

    for (i = 0; i < len; ++i) {
        h += str[i];
        h *= 7;
    }

    return (h | 0x80000000);
}


ngx_int_t
ngx_http_lifecycle_get_local_ip(ngx_str_t device, struct sockaddr_in *addr)
{
    int                 sock;
    struct ifreq        ifr;

    if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        return NGX_ERROR;
    }

    ngx_memcpy(ifr.ifr_name, device.data, device.len);
    ifr.ifr_name[device.len] ='\0';

    if(ioctl(sock, SIOCGIFADDR, &ifr) < 0) {
        close(sock);
        return NGX_ERROR;
    }

    *addr = *((struct sockaddr_in *) &ifr.ifr_addr);

    close(sock);
    return NGX_OK;
}


ngx_buf_t *
ngx_http_lifecycle_copy_buf_chain(ngx_pool_t *pool, ngx_chain_t *in)
{
    ngx_int_t    len;
    ngx_buf_t   *buf;
    ngx_chain_t *cl;


    if (in->next == NULL) {
        return in->buf;
    }

    len = 0;

    for (cl = in; cl; cl = cl->next) {
        len += ngx_buf_size(cl->buf);
    }

    buf = ngx_create_temp_buf(pool, len);

    if (buf == NULL) {
        return NULL;
    }

    for (cl = in; cl; cl = cl->next) {
        buf->last = ngx_copy(buf->last, cl->buf->pos, ngx_buf_size(cl->buf));
    }
    return buf;
}


ngx_int_t
ngx_http_lifecycle_sum_md5(ngx_chain_t *data, u_char *md5_final,
    ssize_t *data_len, ngx_log_t *log)
{
    u_char                       *buf;
    ssize_t                       n, buf_size;
    ngx_md5_t                     md5;

    ngx_md5_init(&md5);

    while(data) {
        if (ngx_buf_in_memory(data->buf)) {
            ngx_md5_update(&md5, data->buf->pos, ngx_buf_size(data->buf));
            *data_len += ngx_buf_size(data->buf);

        } else {
            /* two buf */
            buf_size = ngx_buf_size(data->buf);
            buf = ngx_alloc(buf_size, log);
            if (buf == NULL) {
                return NGX_ERROR;
            }

            n = ngx_read_file(data->buf->file, buf, buf_size, data->buf->file_pos);
            if (n == NGX_ERROR) {
                free(buf);
                return NGX_ERROR;
            }

            if (n != buf_size) {
                ngx_log_error(NGX_LOG_ALERT, log, 0,
                              ngx_read_file_n " read only %z of %O from \"%s\"",
                              n, buf_size, data->buf->file->name.data);
                free(buf);
                return NGX_ERROR;
            }

            ngx_md5_update(&md5, buf, n);
            free(buf);
            *data_len += buf_size;
        }

        data = data->next;
    }

    ngx_md5_final(md5_final, &md5);

    return NGX_OK;
}


u_char *
ngx_http_lifecycle_time(u_char *buf, time_t t)
{
    ngx_tm_t  tm;

    ngx_gmtime(t, &tm);

    return ngx_sprintf(buf, "%s, %02d %s %4d %02d:%02d:%02d GMT",
                       week[tm.ngx_tm_wday],
                       tm.ngx_tm_mday,
                       months[tm.ngx_tm_mon - 1],
                       tm.ngx_tm_year,
                       tm.ngx_tm_hour,
                       tm.ngx_tm_min,
                       tm.ngx_tm_sec);
}


ngx_int_t
ngx_http_lifecycle_get_positive_time(ngx_str_t *buf)
{
    u_char   *str;
    ngx_tm_t  t;
    ngx_int_t positive_time;

    str = buf->data;
    //D2022-10-12T00:00:00
    if (str == NULL || buf->len != 20) {
        return NGX_ERROR;
    }
    if (str[0] != 'D' || str[5] != '-' || str[8] != '-' ||
        str[11] != 'T' || str[14] != ':' || str[17] != ':')
    {
        return NGX_ERROR;
    }
    t.tm_year = (str[1] - '0') * 1000 + (str[2] - '0') * 100 + (str[3] - '0') * 10 + (str[4] - '0') - 1900;
    t.tm_mon = (str[6] - '0') * 10 + (str[7] - '0') - 1;
    t.tm_mday = (str[9] - '0') * 10 + (str[10] - '0');
    t.tm_hour = (str[12] - '0') * 10 + (str[13] - '0');
    t.tm_min = (str[15] - '0') * 10 + (str[16] - '0');
    t.tm_sec = (str[18] - '0') * 10 + (str[19] - '0');
    t.tm_isdst = 0;
    positive_time = mktime(&t);
    return positive_time;
}


ngx_int_t
ngx_http_lifecycle_get_mk_time(ngx_str_t *buf)
{
    u_char   *str, *str_month;
    ngx_tm_t  t;
    ngx_int_t mk_time;
    ngx_uint_t i;

    str = buf->data;
    str_month = str + 8;
    //Fri, 09 Mar 2012 13:40:32 GMT
    if (str == NULL || buf->len != 29) {
        return NGX_ERROR;
    }
    for (i = 0; i < 12; ++i) {
        if (ngx_memcmp(str_month, months[i], 3) == 0) {
            break;
        }
    }

    t.tm_year = (str[12] - '0') * 1000 + (str[13] - '0') * 100 +
                (str[14] - '0') * 10 + (str[15] - '0') - 1900;
    t.tm_mon = i;
    t.tm_mday = (str[5] - '0') * 10 + (str[6] - '0');
    t.tm_hour = (str[17] - '0') * 10 + (str[18] - '0');
    t.tm_min = (str[20] - '0') * 10 + (str[21] - '0');
    t.tm_sec = (str[23] - '0') * 10 + (str[24] - '0');
    t.tm_isdst = 0;
    mk_time = mktime(&t);
    return mk_time;
}



ngx_int_t
ngx_http_lifecycle_get_relative_time(ngx_str_t *buf)
{
    u_char   *str;
    ngx_int_t relative_time, tm_year, tm_mday, tm_hour;

    str = buf->data;
    //Y000D365H24
    if (str == NULL || buf->len != 11) {
        return NGX_ERROR;
    }
    if (str[0] != 'Y' || str[4] != 'D' || str[8] != 'H')
    {
        return NGX_ERROR;
    }
    tm_year = (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0');
    tm_mday = (str[5] - '0') * 100 + (str[6] - '0') * 10 + (str[7] - '0');
    tm_hour = (str[9] - '0') * 10 + (str[10] - '0');

    relative_time = 0;
    relative_time += tm_year * 365 * 24 * 60 * 60;
    relative_time += tm_mday * 24 * 60 * 60;
    relative_time += tm_hour * 60 * 60;

    return relative_time;
}



ngx_int_t
ngx_http_lifecycle_status_message(ngx_buf_t *b, ngx_str_t *action, ngx_log_t *log)
{
    int32_t                                   code, err_len;
    ngx_str_t                                 err;
    ngx_http_lifecycle_status_msg_t                *res;

    res = (ngx_http_lifecycle_status_msg_t *) b->pos;
    err.len = 0;
    code = res->code;

    if (code != NGX_HTTP_LIFECYCLE_STATUS_MESSAGE_OK) {
        err_len = res->error_len;
        if (err_len > 0) {
            err.data = res->error_str;
            err.len = err_len;
        }

        ngx_log_error(NGX_LOG_ERR, log, 0,
                      "%V failed error code (%d) err_msg(%V)", action, code, &err);
        if (code <= NGX_HTTP_LIFECYCLE_EXIT_GENERAL_ERROR) {
            return code;
        }

        return NGX_HTTP_LIFECYCLE_EXIT_GENERAL_ERROR;
    }

    ngx_log_error(NGX_LOG_INFO, log, 0, "%V success ", action);
    return NGX_OK;
}


ngx_int_t
ngx_http_lifecycle_atoull(u_char *line, size_t n, unsigned long long *value)
{
    unsigned long long res;

    for (res = 0; n--; line++) {
        unsigned int val;

        if (*line < '0' || *line > '9') {
            return NGX_ERROR;
        }

        val = *line - '0';

        /*
         * Check for overflow
         */

        if (res & (~0ull << 60)) {

            if (res > ((ULLONG_MAX - val) / 10)) {
                return NGX_ERROR;
            }
        }

        res = res * 10 + val;
    }

    *value = res;

    return NGX_OK;
}


uint64_t
ngx_http_lifecycle_get_chain_buf_size(ngx_chain_t *data)
{
    uint64_t                       size;
    ngx_chain_t                   *cl;

    size = 0;
    cl = data;
    while (cl) {
        size += ngx_buf_size(cl->buf);
        cl = cl->next;
    }

    return size;
}




