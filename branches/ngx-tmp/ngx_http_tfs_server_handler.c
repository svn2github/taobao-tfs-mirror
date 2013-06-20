
/*
 * taobao tfs for nginx
 *
 * This module is designed to support restful interface to tfs
 *
 * Author: diaoliang
 * Email: diaoliang@taobao.com
 */


#include <ngx_http_tfs_errno.h>
#include <ngx_http_tfs_duplicate.h>
#include <ngx_http_tfs_server_handler.h>
#include <ngx_http_tfs_kv_root_server_message.h>
#include <ngx_http_tfs_kv_meta_server_message.h>
#include <ngx_http_tfs_rc_server_message.h>
#include <ngx_http_tfs_name_server_message.h>
#include <ngx_http_tfs_data_server_message.h>
#include <ngx_http_tfs_remote_block_cache.h>
#include <ngx_http_authorize_module.h>


ngx_int_t
ngx_http_tfs_create_auth_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_meta_auth_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_auth(ngx_http_tfs_t *t)
{
    ngx_buf_t                        *b;
    ngx_int_t                         rc;
    ngx_http_tfs_header_t            *header;
    ngx_http_tfs_peer_connection_t   *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_meta_auth_parse_message(t);

    ngx_http_tfs_clear_buf(b);

    ngx_http_authorize_handler(t);

    t->state += 1;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_create_rs_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_root_server_create_message(t->pool);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_rs(ngx_http_tfs_t *t)
{
    ngx_int_t                        rc;
    ngx_buf_t                       *b;
    ngx_http_tfs_inet_t             *addr;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_root_server_parse_message(t);
    if (rc != NGX_OK) {
        return rc;
    }

    t->state += 1;

    rc = ngx_http_tfs_set_custom_initial_parameters(t);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }
    // FIXME: tmp use
    if (rc == NGX_DONE) {
        return NGX_DONE;
    }

    addr = ngx_http_tfs_select_meta_server(t);
    ngx_http_tfs_peer_set_addr(t->pool,
        &t->tfs_peer_servers[NGX_HTTP_TFS_META_SERVER], addr);

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ms_get_bucket(ngx_http_tfs_t *t)
{
    size_t                                   size, last_common_prefix_len, marker_len;
    uint32_t                                 i, j;
    ngx_buf_t                               *b;
    ngx_int_t                                rc, cmp_rc;
    ngx_chain_t                             *cl, **ll;
    ngx_http_tfs_peer_connection_t          *tp;
    ngx_http_tfs_custom_file_info_t         *custom_file_info;
    ngx_http_tfs_object_meta_info_t         *object_meta_info;
    ngx_http_tfs_custom_file_info_chain_t  **cc, *new_cc;
    ngx_http_tfs_object_meta_info_chain_t   *oc;

    tp = t->tfs_peer;
    b = &tp->body_buffer;

    size = ngx_buf_size(b);
    /* do not process until all data recvd */
    if (size < (size_t)t->length) {
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_meta_server_parse_message(t);

    ngx_http_tfs_clear_buf(b);

    if (rc != NGX_OK) {
        if (rc == NGX_HTTP_TFS_EXIT_BUCKET_NOT_EXIST) {
            t->orig_action = t->r_ctx.action.code;
            t->r_ctx.action.code = NGX_HTTP_TFS_ACTION_PUT_BUCKET;
            return NGX_OK;
        }

        /* concurrent put bucket occurs when put object */
        if (rc == NGX_HTTP_TFS_EXIT_BUCKET_EXIST
            && t->orig_action != NGX_HTTP_TFS_ACTION_NON)
        {
            t->r_ctx.action.code = t->orig_action;
            return NGX_OK;
        }

        return rc;
    }

    switch (t->r_ctx.action.code) {
    case NGX_HTTP_TFS_ACTION_GET_BUCKET:
        /* parse to ls_dir result, assume object_name_count == object_meta_count */
        if (t->get_bucket_ctx.object_name_count > 0
            || t->get_bucket_ctx.common_prefix_count > 0)
        {
            new_cc = t->get_bucket_ctx.custom_file_infos;
            cc = &t->get_bucket_ctx.custom_file_infos;
            for(; new_cc; new_cc = new_cc->next) {
                cc = &new_cc->next;
            }

            new_cc = ngx_pcalloc(t->pool, sizeof(ngx_http_tfs_custom_file_info_chain_t));
            if (new_cc == NULL) {
                return NGX_ERROR;
            }

            /* common prefix will be parsed as subdir */
            new_cc->file_count = t->get_bucket_ctx.object_name_count + t->get_bucket_ctx.common_prefix_count;
            new_cc->file_info = ngx_pcalloc(t->pool,
                sizeof(ngx_http_tfs_custom_file_info_t) * new_cc->file_count);
            if (new_cc->file_info == NULL) {
                return NGX_ERROR;
            }
            *cc = new_cc;

            custom_file_info = new_cc->file_info;

            /* find last object_meta_infos */
            oc = t->get_bucket_ctx.object_meta_infos;
            if (oc == NULL) {
                return NGX_ERROR;
            }
            for(; oc->next; oc = oc->next);

            object_meta_info = oc->meta_info;

            ngx_log_debug2(NGX_LOG_DEBUG_HTTP, t->log, 0,
                           "get_bucket success, object_count: %d, "
                           "common_prefix_count: %d",
                           t->get_bucket_ctx.object_name_count,
                           t->get_bucket_ctx.common_prefix_count);

            ngx_str_null(&t->get_bucket_ctx.marker);
            for (i = 0; i < t->get_bucket_ctx.object_name_count; i++) {
                custom_file_info[i].name = t->get_bucket_ctx.object_name[i];
                custom_file_info[i].create_time = (uint32_t)object_meta_info[i].create_time;
                custom_file_info[i].modify_time = (uint32_t)object_meta_info[i].modify_time;
                custom_file_info[i].size = object_meta_info[i].big_file_size;
                custom_file_info[i].pid |= (0x01L << 63);
            }
            /* marker may be the last object name */
            if (t->get_bucket_ctx.object_name_count > 0) {
                t->get_bucket_ctx.marker = t->get_bucket_ctx.object_name[i - 1];
            }

            /* subdirs */
            for (j = 0; j < t->get_bucket_ctx.common_prefix_count; j++, i++) {
                custom_file_info[i].name = t->get_bucket_ctx.common_prefix[j];
                /* remove last delimiter('/') */
                custom_file_info[i].name.len -= 1;
            }
            /* marker may be the last common prefix */
            if (t->get_bucket_ctx.common_prefix_count > 0) {
                if (t->get_bucket_ctx.marker.len == 0) {
                    t->get_bucket_ctx.marker = t->get_bucket_ctx.common_prefix[j - 1];

                } else {
                    /* alphabetical order compare */
                    last_common_prefix_len = t->get_bucket_ctx.common_prefix[j - 1].len;
                    marker_len = t->get_bucket_ctx.marker.len;
                    cmp_rc = ngx_strncmp(t->get_bucket_ctx.common_prefix[j - 1].data,
                                         t->get_bucket_ctx.marker.data,
                                         ngx_min(last_common_prefix_len, marker_len));
                    if (cmp_rc > 0) {
                        t->get_bucket_ctx.marker = t->get_bucket_ctx.common_prefix[j - 1];

                    } else if (cmp_rc == 0 && last_common_prefix_len > marker_len) {
                        t->get_bucket_ctx.marker = t->get_bucket_ctx.common_prefix[j - 1];
                    }
                }
            }

            /* remove prefix from name */
            if (t->get_bucket_ctx.prefix.len > 0) {
                for (i = 0; i < new_cc->file_count; i++) {
                    custom_file_info[i].name.data += t->get_bucket_ctx.prefix.len;
                    custom_file_info[i].name.len -= t->get_bucket_ctx.prefix.len;
                }
            }
        }

        if (!t->file.still_have) {
            if (t->get_bucket_ctx.custom_file_infos != NULL
                && t->get_bucket_ctx.custom_file_infos->file_count > 0)
            {
                /* need json output */
                for (cl = t->out_bufs, ll = &t->out_bufs; cl; cl = cl->next) {
                    ll = &cl->next;
                }

                cl = ngx_http_tfs_json_custom_file_info(t->json_output,
                                                        t->get_bucket_ctx.custom_file_infos,
                                                        t->r_ctx.file_type);
                if (cl == NULL) {
                    return NGX_ERROR;
                }

                *ll = cl;
            }

            t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
            return NGX_DONE;
        }
        break;

    case NGX_HTTP_TFS_ACTION_REMOVE_DIR:
        /* return success if dir is empty */
        if (t->get_bucket_ctx.object_name_count > 0
            || t->get_bucket_ctx.common_prefix_count > 0)
        {
            t->tfs_status = NGX_HTTP_TFS_EXIT_DELETE_DIR_WITH_FILE_ERROR;

        } else {
            t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        }

        return NGX_DONE;
    /*
    case NGX_HTTP_TFS_ACTION_GET_BUCKET:
        // TODO:
        break;
    */
    default:
        return NGX_ERROR;
    }

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_create_ms_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_meta_server_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ms(ngx_http_tfs_t *t)
{
    int64_t                           curr_length;
    uint16_t                          action;
    uint32_t                          segment_count, i;
    uint64_t                          end_offset;
    ngx_buf_t                        *b;
    ngx_int_t                         rc;
    ngx_chain_t                      *cl, **ll;
    ngx_http_tfs_header_t            *header;
    ngx_http_tfs_file_info_t         *tfs_file_infos;
    ngx_http_tfs_segment_data_t      *segment_data, *first_segment, *last_segment;
    ngx_http_tfs_file_hole_info_t    *file_hole_info;
    ngx_http_tfs_peer_connection_t   *tp;
    ngx_http_tfs_custom_file_info_t  *custom_file_info;
    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    action = t->r_ctx.action.code;
    rc = ngx_http_tfs_meta_server_parse_message(t);
    ngx_log_error(NGX_LOG_INFO, t->log, 0, "into==== ngx_http_tfs_meta_server_parse_message %d rc",rc);
    ngx_http_tfs_clear_buf(b);

    if (rc != NGX_OK) {
        /* need put bucket */
        if (rc == NGX_HTTP_TFS_EXIT_BUCKET_NOT_EXIST) {
            t->orig_action = t->r_ctx.action.code;
            t->orig_state = t->state;
            t->r_ctx.action.code = NGX_HTTP_TFS_ACTION_PUT_BUCKET;
            t->state = NGX_HTTP_TFS_STATE_ACTION_PROCESS;
            return NGX_OK;
        }

        /* concurrent put bucket occurs when put object */
        if (rc == NGX_HTTP_TFS_EXIT_BUCKET_EXIST
            && t->orig_action != NGX_HTTP_TFS_ACTION_NON)
        {
            t->r_ctx.action.code = t->orig_action;
            t->state = t->orig_state;
            return NGX_OK;
        }

        return rc;
    }

    switch (action) {
    case NGX_HTTP_TFS_ACTION_PUT_BUCKET:
        if (t->orig_action != NGX_HTTP_TFS_ACTION_NON) {
            t->r_ctx.action.code = t->orig_action;
            t->state = t->orig_state;

        } else {
            rc = NGX_DONE;
        }
        t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        return rc;

    case NGX_HTTP_TFS_ACTION_DEL_BUCKET:
        t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        return NGX_DONE;

    case NGX_HTTP_TFS_ACTION_HEAD_BUCKET:
        ngx_log_error(NGX_LOG_INFO, t->log, 0, "into==== zhuanhuan");
        t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        return NGX_DONE;

    case NGX_HTTP_TFS_ACTION_CREATE_FILE:
        t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        return NGX_DONE;

    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        if (t->file.left_length == 0) {
            t->state = NGX_HTTP_TFS_STATE_WRITE_DONE;
            return NGX_DONE;
        }
        t->state = NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO;
        break;

    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        // FIXME: no need to compare every time
        t->file.left_length = ngx_min(t->file.left_length,
            (t->file.object_info.meta_info.big_file_size - t->file.file_offset));
        segment_data = NULL;
        segment_count = t->file.object_info.tfs_file_count;
        if (segment_count > 0) {
            if (t->file.segment_data == NULL) {
                t->file.segment_data = ngx_pcalloc(t->pool,
                    sizeof(ngx_http_tfs_segment_data_t) * segment_count);
                if (t->file.segment_data == NULL) {
                    return NGX_ERROR;
                }

            } else {
                /* need realloc */
                if (segment_count > t->file.segment_count) {
                    t->file.segment_data = ngx_prealloc(t->pool, t->file.segment_data,
                        sizeof(ngx_http_tfs_segment_data_t) * t->file.segment_count,
                        sizeof(ngx_http_tfs_segment_data_t) * segment_count);
                    if (t->file.segment_data == NULL) {
                        return NGX_ERROR;
                    }
                }
                /* reuse */
                ngx_memzero(t->file.segment_data,
                            sizeof(ngx_http_tfs_segment_data_t) * segment_count);
            }

            t->file.segment_count = segment_count;
            t->file.segment_index = 0;

            tfs_file_infos = t->file.object_info.tfs_file_infos;
            segment_data = t->file.segment_data;
            for (i = 0; i < segment_count; i++) {
                segment_data[i].segment_info.block_id = tfs_file_infos[i].block_id;
                segment_data[i].segment_info.file_id = tfs_file_infos[i].file_id;
                segment_data[i].segment_info.offset = tfs_file_infos[i].offset;
                segment_data[i].segment_info.size = tfs_file_infos[i].size;
                segment_data[i].oper_size = tfs_file_infos[i].size;
            }

            /* first semgent's oper_offset and oper_size are special for pread */
            first_segment = &t->file.segment_data[0];
            if (t->file.file_offset > first_segment->segment_info.offset) {
                first_segment->oper_offset = t->file.file_offset -
                    first_segment->segment_info.offset;
                first_segment->oper_size = first_segment->segment_info.size -
                    first_segment->oper_offset;
            }
            /* if last segment(also special) has been readed, set its oper_size*/
            /* notice that it maybe first segment */
            if (!t->file.still_have) {
                last_segment = &t->file.segment_data[segment_count - 1];
                end_offset = t->file.file_offset + t->file.left_length;
                /* ensure end_offset larger than last_segment's absolute offset */
                last_segment->oper_size =
                    ngx_min((end_offset - (last_segment->segment_info.offset +
                                           last_segment->oper_offset)),
                            last_segment->segment_info.size);
            }
        }

        if (t->r_ctx.chk_file_hole) {
            if (segment_data != NULL) {
                for (i = 0; i < segment_count; i++, segment_data++) {
                    if (t->file.file_offset < segment_data->segment_info.offset) {
                        curr_length = ngx_min(t->file.left_length,
                            (uint64_t)(segment_data->segment_info.offset - t->file.file_offset));
                        file_hole_info = ngx_array_push(&t->file_holes);
                        if (file_hole_info == NULL) {
                            return NGX_ERROR;
                        }

                        file_hole_info->offset = t->file.file_offset;
                        file_hole_info->length = curr_length;

                        ngx_log_error(NGX_LOG_DEBUG, t->log, 0,
                                      "find file hole, offset: %uL, length: %uL",
                                      file_hole_info->offset, file_hole_info->length);

                        t->file.file_offset += curr_length;
                        t->file.left_length -= curr_length;
                        if (t->file.left_length == 0) {
                            break;
                        }
                    }
                    t->file.file_offset += segment_data->oper_size;
                    t->file.left_length -= segment_data->oper_size;
                    if (t->file.left_length == 0) {
                        break;
                    }
                }
            }

            if (!t->file.still_have) {
                /* left is all file hole(beyond last segment) */
                if (t->file.left_length > 0) {
                    file_hole_info = ngx_array_push(&t->file_holes);
                    if (file_hole_info == NULL) {
                        return NGX_ERROR;
                    }

                    file_hole_info->offset = t->file.file_offset;
                    file_hole_info->length = t->file.left_length;

                    ngx_log_error(NGX_LOG_DEBUG, t->log, 0,
                                  "find file hole, offset: %uL, length: %uL",
                                  file_hole_info->offset, file_hole_info->length);
                    t->file.file_offset += t->file.left_length;
                    t->file.left_length = 0;
                }

                /* need json output */
                if (t->file_holes.nelts > 0) {
                    t->json_output = ngx_http_tfs_json_init(t->log, t->pool);
                    if (t->json_output == NULL) {
                        return NGX_ERROR;
                    }

                    for (cl = t->out_bufs, ll = &t->out_bufs; cl; cl = cl->next) {
                        ll = &cl->next;
                    }

                    cl = ngx_http_tfs_json_file_hole_info(t->json_output,
                                                          &t->file_holes);
                    if (cl == NULL) {
                        return NGX_ERROR;
                    }

                    *ll = cl;
                }

                t->state = NGX_HTTP_TFS_STATE_READ_DONE;
                return NGX_DONE;
            }
            return NGX_OK;
        }

        /* read */
        if (segment_count == 0) {
            if (t->file.left_length > 0) {
                /* left is all file hole(beyond last segment) */
                rc = ngx_http_tfs_fill_file_hole(t, t->file.left_length);
                t->stat_info.size += t->file.left_length;
            }

            t->state = NGX_HTTP_TFS_STATE_READ_DONE;
            return NGX_DONE;
        }

#if (NGX_DEBUG)
        for (i = 0; i < segment_count; i++) {
            ngx_log_debug3(NGX_LOG_DEBUG_HTTP, t->log, 0,
                           "segment index: %d, oper_offset: %uD, oper_size: %uD",
                           i, segment_data[i].oper_offset, segment_data[i].oper_size);
        }
#endif
        rc = NGX_OK;

        t->state = NGX_HTTP_TFS_STATE_READ_GET_BLK_INFO;
        break;

    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        segment_count = t->file.object_info.tfs_file_count;
        if (segment_count == 0) {
            t->state = NGX_HTTP_TFS_STATE_REMOVE_DONE;
            return NGX_DONE;
        }

        if (t->file.segment_data == NULL) {
            t->file.segment_data = ngx_pcalloc(t->pool,
                sizeof(ngx_http_tfs_segment_data_t) * segment_count);
            if (t->file.segment_data == NULL) {
                return NGX_ERROR;
            }

        } else {
            /* need realloc */
            if (segment_count > t->file.segment_count) {
                t->file.segment_data = ngx_prealloc(t->pool, t->file.segment_data,
                    sizeof(ngx_http_tfs_segment_data_t) * t->file.segment_count,
                    sizeof(ngx_http_tfs_segment_data_t) * segment_count);
                if (t->file.segment_data == NULL) {
                    return NGX_ERROR;
                }
            }
            /* reuse */
            ngx_memzero(t->file.segment_data,
                        sizeof(ngx_http_tfs_segment_data_t) * segment_count);
        }

        t->file.segment_count = segment_count;
        t->file.segment_index = 0;

        tfs_file_infos = t->file.object_info.tfs_file_infos;
        segment_data = t->file.segment_data;
        for (i = 0; i < segment_count; i++) {
            segment_data[i].segment_info.block_id = tfs_file_infos[i].block_id;
            segment_data[i].segment_info.file_id = tfs_file_infos[i].file_id;
            segment_data[i].segment_info.offset = tfs_file_infos[i].offset;
            segment_data[i].segment_info.size = tfs_file_infos[i].size;
        }

        t->state = NGX_HTTP_TFS_STATE_REMOVE_GET_BLK_INFO;
        rc = NGX_OK;
        break;

    case NGX_HTTP_TFS_ACTION_HEAD_OBJECT:
        if (!t->r_ctx.chk_exist && t->get_bucket_ctx.custom_file_infos->file_count > 0) {
            /* set file's pid */
            custom_file_info = t->get_bucket_ctx.custom_file_infos->file_info;
            custom_file_info->pid |= (0x01L << 63);

            /* need json output */
            for (cl = t->out_bufs, ll = &t->out_bufs; cl; cl = cl->next) {
                ll = &cl->next;
            }

            cl = ngx_http_tfs_json_custom_file_info(t->json_output,
                                                    t->get_bucket_ctx.custom_file_infos,
                                                    t->r_ctx.file_type);
            if (cl == NULL) {
                return NGX_ERROR;
            }

            *ll = cl;
        }

        t->state = NGX_HTTP_TFS_STATE_ACTION_DONE;
        return NGX_DONE;

    default:
        return NGX_ERROR;
    }

    /* only select once */
    if (t->name_server_addr_text.len == 0) {
        rc = ngx_http_tfs_select_name_server(t, t->rc_info_node, &t->name_server_addr,
                                             &t->name_server_addr_text);
        if (rc == NGX_ERROR) {
            return NGX_HTTP_TFS_EIXT_SERVER_OBJECT_NOT_FOUND;  /* in order to return 404 */
        }

        ngx_http_tfs_peer_set_addr(t->pool,
            &t->tfs_peer_servers[NGX_HTTP_TFS_NAME_SERVER], &t->name_server_addr);
    }

    if (t->r_ctx.action.code == NGX_HTTP_TFS_ACTION_GET_OBJECT)
    {
        /* lookup block cache */
        t->block_cache_ctx.curr_lookup_cache = NGX_HTTP_TFS_LOCAL_BLOCK_CACHE;
        t->decline_handler = ngx_http_tfs_batch_lookup_block_cache;
        return NGX_DECLINED;
    }

    return rc;
}


ngx_int_t
ngx_http_tfs_retry_ms(ngx_http_tfs_t *t)
{
    ngx_http_tfs_inet_t             *addr;
    ngx_http_tfs_peer_connection_t  *tp;

    tp = t->tfs_peer;
    tp->peer.free(&tp->peer, tp->peer.data, 0);

    addr = ngx_http_tfs_select_meta_server(t);
    // TODO: return what?
    if (addr == NULL) {
        return NGX_ERROR;
    }

    ngx_http_tfs_peer_set_addr(t->pool,
        &t->tfs_peer_servers[NGX_HTTP_TFS_META_SERVER], addr);

    if (ngx_http_tfs_reinit(t->data, t) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_http_tfs_connect(t);

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_create_rcs_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_rc_server_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_rcs(ngx_http_tfs_t *t)
{
    ngx_buf_t                       *b;
    ngx_int_t                        rc;
    ngx_http_tfs_rc_ctx_t           *rc_ctx;
    ngx_http_tfs_rcs_info_t         *rc_info;
    ngx_http_tfs_peer_connection_t  *tp;

    tp = t->tfs_peer;
    b = &tp->body_buffer;
    rc_ctx = t->main_conf->rc_ctx;

    rc = ngx_http_tfs_rc_server_parse_message(t);

    ngx_http_tfs_clear_buf(b);

    if (t->r_ctx.action.code == NGX_HTTP_TFS_ACTION_KEEPALIVE) {
        if (t->curr_ka_queue == ngx_queue_sentinel(&rc_ctx->sh->kp_queue)) {
            rc = NGX_DONE;
        }

        return rc;
    }

    if (rc == NGX_ERROR || rc <= NGX_HTTP_TFS_EXIT_GENERAL_ERROR) {
        return rc;
    }

    rc_info = t->rc_info_node;

    if (t->r_ctx.action.code == NGX_HTTP_TFS_ACTION_GET_APPID) {
        rc = ngx_http_tfs_set_output_appid(t, rc_info->app_id);
        if (rc == NGX_ERROR) {
            return NGX_ERROR;
        }
        return NGX_DONE;
    }

    // TODO: use fine granularity mutex(per rc_info_node mutex)
    //ngx_shmtx_lock(&rc_ctx->shpool->mutex);
    rc = ngx_http_tfs_misc_ctx_init(t, rc_info);
    //ngx_shmtx_unlock(&rc_ctx->shpool->mutex);

    return rc;
}


ngx_int_t
ngx_http_tfs_create_ns_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_name_server_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ns(ngx_http_tfs_t *t)
{
    ngx_buf_t                        *b;
    ngx_int_t                         rc;
    ngx_http_tfs_inet_t              *addr;
    ngx_http_tfs_header_t            *header;
    ngx_http_tfs_rcs_info_t          *rc_info;
    ngx_http_tfs_peer_connection_t   *tp;
    ngx_http_tfs_logical_cluster_t   *logical_cluster;
    ngx_http_tfs_physical_cluster_t  *physical_cluster;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    b = &tp->body_buffer;

    if (ngx_buf_size(b) < header->len) {
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_name_server_parse_message(t);

    ngx_http_tfs_clear_buf(b);
    if (rc == NGX_ERROR) {
        return rc;
    }

    if (rc <= NGX_HTTP_TFS_EXIT_GENERAL_ERROR
        || rc == NGX_HTTP_TFS_AGAIN)
    {
        return NGX_HTTP_TFS_AGAIN;
    }

    switch (t->r_ctx.action.code) {

    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        if (!t->parent
            && (t->r_ctx.version == 2
                || (t->is_large_file && !t->is_process_meta_seg)))
        {
            t->decline_handler = ngx_http_tfs_batch_process_start;
            return NGX_DECLINED;
        }
        t->state = NGX_HTTP_TFS_STATE_READ_READ_DATA;
        break;

    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        switch(t->state) {
        case NGX_HTTP_TFS_STATE_WRITE_GET_CLUSTER_ID:
            /* save cluster id in rc_info */
            rc_info = t->rc_info_node;
            logical_cluster = &rc_info->logical_clusters[t->logical_cluster_index];
            physical_cluster = &logical_cluster->rw_clusters[t->rw_cluster_index];
            /* check ns cluster id with rc configure */
            if (t->file.cluster_id !=
                (uint32_t)ngx_http_tfs_get_cluster_id(physical_cluster->cluster_id_text.data))
            {
                ngx_log_error(NGX_LOG_ERR, t->log, 0,
                              "error, cluster id conflict: %uD(ns) <> %uD(rcs)",
                              t->file.cluster_id,
                              ngx_http_tfs_get_cluster_id(physical_cluster->cluster_id_text.data));
                return NGX_ERROR;
            }
            physical_cluster->cluster_id = t->file.cluster_id;
            t->state = NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO;
            return rc;

        case NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO:
            if (t->is_stat_dup_file) {
                t->state = NGX_HTTP_TFS_STATE_WRITE_STAT_DUP_FILE;

            } else if (t->is_rolling_back) {
                t->state = NGX_HTTP_TFS_STATE_WRITE_DELETE_DATA;

            } else {
                if (!t->parent
                    && (t->r_ctx.version == 2
                        || (t->is_large_file && !t->is_process_meta_seg)))
                {
                    t->decline_handler = ngx_http_tfs_batch_process_start;
                    return NGX_DECLINED;
                }
                t->state = NGX_HTTP_TFS_STATE_WRITE_CREATE_FILE_NAME;
            }
            break;

        default:
            break;
        }
        break;

    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        switch (t->state) {
        case NGX_HTTP_TFS_STATE_REMOVE_GET_GROUP_COUNT:
            if (t->group_count != 1) {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_GET_GROUP_SEQ;
                return rc;
            }
            /* group_count == 1, maybe able to make choice */
            t->group_seq = 0;
            /* no break here */
        case NGX_HTTP_TFS_STATE_REMOVE_GET_GROUP_SEQ:
            rc_info = t->rc_info_node;
            ngx_http_tfs_rcs_set_group_info_by_addr(rc_info,
                                                    t->group_count,
                                                    t->group_seq,
                                                    t->name_server_addr);
            rc = ngx_http_tfs_select_name_server(t, rc_info,
                                                 &t->name_server_addr,
                                                 &t->name_server_addr_text);
            if (rc == NGX_ERROR) {
                return NGX_HTTP_TFS_EIXT_SERVER_OBJECT_NOT_FOUND;
            }

            tp->peer.free(&tp->peer, tp->peer.data, 0);

            ngx_http_tfs_peer_set_addr(t->pool,
                                       &t->tfs_peer_servers[NGX_HTTP_TFS_NAME_SERVER],
                                       &t->name_server_addr);
            return rc;

        case NGX_HTTP_TFS_STATE_REMOVE_GET_BLK_INFO:
            if (t->is_large_file
                && t->r_ctx.unlink_type == NGX_HTTP_TFS_UNLINK_DELETE
                && t->meta_segment_data == NULL)
            {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_READ_META_SEGMENT;

            } else if (t->is_stat_dup_file) {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_STAT_FILE;

            } else {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_DELETE_DATA;
            }
            break;

        default:
            break;
        }
        break;

    default:
        break;
    }

    addr = ngx_http_tfs_select_data_server(t,
        &t->file.segment_data[t->file.segment_index]);
    if (addr == NULL) {
        return NGX_ERROR;
    }

    ngx_http_tfs_peer_set_addr(t->pool,
        &t->tfs_peer_servers[NGX_HTTP_TFS_DATA_SERVER], addr);
    return rc;
}


void
ngx_http_tfs_reset_segment_data(ngx_http_tfs_t *t)
{
    uint32_t                     block_count, i;
    ngx_http_tfs_segment_data_t  *segment_data;

    /* reset current lookup cache */
    t->block_cache_ctx.curr_lookup_cache = NGX_HTTP_TFS_LOCAL_BLOCK_CACHE;

    block_count = t->file.segment_count - t->file.segment_index;
    if (block_count > NGX_HTTP_TFS_MAX_BATCH_COUNT) {
        block_count = NGX_HTTP_TFS_MAX_BATCH_COUNT;
    }

    segment_data = &t->file.segment_data[t->file.segment_index];
    for (i = 0; i < block_count; i++, segment_data++) {
        segment_data->cache_hit = NGX_HTTP_TFS_NO_BLOCK_CACHE;
        segment_data->block_info_src = NGX_HTTP_TFS_FROM_NONE;
        segment_data->block_info.ds_addrs = NULL;
        segment_data->ds_retry = 0;
        segment_data->ds_index = 0;
    }
}


ngx_int_t
ngx_http_tfs_retry_ns(ngx_http_tfs_t *t)
{
    ngx_int_t                        rc;
    ngx_http_tfs_peer_connection_t  *tp;

    if (!t->retry_curr_ns) {
        t->rw_cluster_index++;
        rc = ngx_http_tfs_select_name_server(t, t->rc_info_node,
                                             &t->name_server_addr,
                                             &t->name_server_addr_text);
        if (rc == NGX_ERROR) {
            return NGX_HTTP_TFS_EIXT_SERVER_OBJECT_NOT_FOUND;
        }

        tp = t->tfs_peer;
        tp->peer.free(&tp->peer, tp->peer.data, 0);

        ngx_http_tfs_peer_set_addr(t->pool,
            &t->tfs_peer_servers[NGX_HTTP_TFS_NAME_SERVER],
            &t->name_server_addr);

        ngx_http_tfs_reset_segment_data(t);

    } else {
        t->retry_curr_ns = NGX_HTTP_TFS_NO;
    }

    switch (t->r_ctx.action.code) {
    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        /* lookup block cache */
        if (t->block_cache_ctx.curr_lookup_cache != NGX_HTTP_TFS_NO_BLOCK_CACHE) {
            if (!t->parent
                && (t->r_ctx.version == 2
                    || (t->is_large_file && !t->is_process_meta_seg)))
            {
                t->decline_handler = ngx_http_tfs_batch_lookup_block_cache;

            } else {
                t->decline_handler = ngx_http_tfs_lookup_block_cache;
            }

            return t->decline_handler(t);
        }
        break;

    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        /* stat failed, do not dedup, save new tfs file and do not save tair */
        if (t->is_stat_dup_file) {
            t->is_stat_dup_file = NGX_HTTP_TFS_NO;
            t->use_dedup = NGX_HTTP_TFS_NO;
            t->state = NGX_HTTP_TFS_STATE_WRITE_GET_CLUSTER_ID;
            t->file.segment_data[0].segment_info.block_id = 0;
            t->file.segment_data[0].segment_info.file_id = 0;
        }
        break;

    default:
        break;
    }

    if (ngx_http_tfs_reinit(t->data, t) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_http_tfs_connect(t);

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_create_ds_request(ngx_http_tfs_t *t)
{
    ngx_chain_t  *cl;

    cl = ngx_http_tfs_data_server_create_message(t);
    if (cl == NULL) {
        return NGX_ERROR;
    }

    t->request_bufs = cl;

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ds(ngx_http_tfs_t *t)
{
    size_t                           b_size;
    uint32_t                         body_len, len_to_update;
    ngx_int_t                        rc;
    ngx_buf_t                       *b;
    ngx_chain_t                     *cl, **ll;
    ngx_http_request_t              *r;
    ngx_peer_connection_t           *p;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_segment_data_t     *segment_data;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    tp = t->tfs_peer;
    p = &tp->peer;
    b = &tp->body_buffer;

    body_len = header->len;
    if (ngx_buf_size(b) < body_len) {
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_data_server_parse_message(t);
    if (rc == NGX_ERROR) {
        return NGX_ERROR;
    }

    ngx_http_tfs_clear_buf(b);

    segment_data = &t->file.segment_data[t->file.segment_index];

    switch (t->r_ctx.action.code) {

    case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
        switch(t->state) {
        case NGX_HTTP_TFS_STATE_WRITE_STAT_DUP_FILE:
            if (rc == NGX_OK) {
                if (t->file_info.flag == NGX_HTTP_TFS_FILE_NORMAL) {
                    rc = ngx_http_tfs_set_output_file_name(t);
                    if (rc == NGX_ERROR) {
                        return NGX_ERROR;
                    }

                    r = t->data;
                    t->dedup_ctx.file_data = r->request_body->bufs;
                    t->dedup_ctx.file_ref_count += 1;
                    t->decline_handler = ngx_http_tfs_set_duplicate_info;
                    return NGX_DECLINED;
                }

            } else {
                /* stat success but file is deleted or concealed */
                /* need save new tfs file, but do not save tair */
                if (rc == NGX_HTTP_TFS_EXIT_FILE_STATUS_ERROR ||
                    rc == NGX_HTTP_TFS_EXIT_META_NOT_FOUND_ERROR)
                {
                    t->state = NGX_HTTP_TFS_STATE_WRITE_GET_CLUSTER_ID;
                    t->is_stat_dup_file = NGX_HTTP_TFS_NO;
                    t->use_dedup = NGX_HTTP_TFS_NO;
                    /* need reset block id and file id */
                    t->file.segment_data[0].segment_info.block_id = 0;
                    t->file.segment_data[0].segment_info.file_id = 0;
                    rc = NGX_OK;

                } else {
                    /* stat failed will goto retry */
                    rc = NGX_HTTP_TFS_AGAIN;
                }
            }
            break;

        case NGX_HTTP_TFS_STATE_WRITE_CREATE_FILE_NAME:
            if (rc == NGX_OK) {
                t->state = NGX_HTTP_TFS_STATE_WRITE_WRITE_DATA;
            }
            break;

        case NGX_HTTP_TFS_STATE_WRITE_WRITE_DATA:
            if (rc != NGX_OK) {
                return rc;
            }

            /* write success, update data buf, offset and crc */
            cl = segment_data->data;
            len_to_update = segment_data->oper_size;
            while (len_to_update > 0) {
                while (cl && ngx_buf_size(cl->buf) == 0) {
                    cl = cl->next;
                }
                if (cl == NULL) {
                    ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                  "update send data offset failed for early end.");
                    return NGX_ERROR;
                }
                b_size = ngx_min(ngx_buf_size(cl->buf), len_to_update);
                if (ngx_buf_in_memory(cl->buf)) {
                    cl->buf->pos += b_size;

                } else {
                    cl->buf->file_pos += b_size;
                }
                len_to_update -= b_size;
            }
            segment_data->data = cl;

            t->file.left_length -= segment_data->oper_size;
            t->stat_info.size += segment_data->oper_size;
            segment_data->oper_offset += segment_data->oper_size;
            segment_data->oper_size = ngx_min(t->file.left_length,
                                              NGX_HTTP_TFS_MAX_FRAGMENT_SIZE);
            segment_data->segment_info.crc = segment_data->curr_crc;

            if (t->r_ctx.version == 1) {
                if (t->file.left_length > 0 && !t->is_large_file) {
                    t->state = NGX_HTTP_TFS_STATE_WRITE_WRITE_DATA;
                    return NGX_OK;
                }
            }
            t->state = NGX_HTTP_TFS_STATE_WRITE_CLOSE_FILE;
            break;

        case NGX_HTTP_TFS_STATE_WRITE_CLOSE_FILE:
            if (rc != NGX_OK) {
                return rc;
            }

            /* sub process return here */
            if (t->parent) {
                return NGX_DONE;
            }

            t->file.segment_index++;

            /* small file or large_file meta segment */
            if (t->r_ctx.version == 1) {
                /* client abort need roll back, remove all segments written */
                if (t->client_abort) {
                    t->state = NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO;
                    t->is_rolling_back = NGX_HTTP_TFS_YES;
                    t->file.segment_index = 0;
                    return NGX_OK;
                }

                t->state = NGX_HTTP_TFS_STATE_WRITE_DONE;
                rc = ngx_http_tfs_set_output_file_name(t);
                if (rc == NGX_ERROR) {
                    return NGX_ERROR;
                }
                /* when new tfs file is saved, do not care saving tair is success or not */
                if (t->use_dedup) {
                    r = t->data;
                    t->dedup_ctx.file_data = r->request_body->bufs;
                    t->dedup_ctx.file_ref_count += 1;
                    t->decline_handler = ngx_http_tfs_set_duplicate_info;
                    return NGX_DECLINED;
                }
                return NGX_DONE;
            }
            break;

        /* is rolling back */
        case NGX_HTTP_TFS_STATE_WRITE_DELETE_DATA:
            t->file.segment_index++;
            if (t->file.segment_index >= t->file.segment_count) {
                if (t->client_abort) {
                    return NGX_HTTP_CLIENT_CLOSED_REQUEST;
                }

                if (t->request_timeout) {
                    return NGX_HTTP_REQUEST_TIME_OUT;
                }

                return NGX_ERROR;
            }

            t->state = NGX_HTTP_TFS_STATE_WRITE_GET_BLK_INFO;
            return NGX_OK;
        }
        break;

    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        switch(t->state) {
        case NGX_HTTP_TFS_STATE_REMOVE_STAT_FILE:
            if (rc == NGX_OK) {
                if (t->file_info.flag == NGX_HTTP_TFS_FILE_NORMAL
                    || t->file_info.flag == NGX_HTTP_TFS_FILE_CONCEAL)
                {
                    t->state = NGX_HTTP_TFS_STATE_REMOVE_READ_META_SEGMENT;
                    segment_data->oper_size =
                        ngx_min(t->file_info.size, NGX_HTTP_TFS_MAX_READ_FILE_SIZE);
                    return NGX_OK;
                }

                /* file is deleted */
                return NGX_HTTP_TFS_EXIT_FILE_STATUS_ERROR;
            }
            /* stat failed will goto retry */
            return NGX_HTTP_TFS_AGAIN;

       case NGX_HTTP_TFS_STATE_REMOVE_DELETE_DATA:
            if (rc != NGX_OK) {
                return rc;
            }

            /* small file */
            if (t->r_ctx.version == 1 && !t->is_large_file) {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_DONE;
                t->file_name = t->r_ctx.file_path_s;
                return NGX_DONE;
            }

            /* large_file && custom file */
            t->file.segment_index++;
            if (t->file.segment_index >= t->file.segment_count) {
                if (t->r_ctx.version == 1) {
                    /* large file */
                    t->state = NGX_HTTP_TFS_STATE_REMOVE_DONE;
                    t->file_name = t->r_ctx.file_path_s;
                    return NGX_DONE;
                }

                if (t->r_ctx.version == 2) {
                    if (!t->file.still_have) {
                        t->state = NGX_HTTP_TFS_STATE_REMOVE_DONE;
                        return NGX_DONE;
                    }

                    t->state = NGX_HTTP_TFS_STATE_REMOVE_DEL_OBJECT_INFO;
                    b = &t->tfs_peer_servers[NGX_HTTP_TFS_META_SERVER].body_buffer;
                }

            } else {
                t->state = NGX_HTTP_TFS_STATE_REMOVE_GET_BLK_INFO;
            }
            break;
        }
    }

    return rc;
}


ngx_int_t
ngx_http_tfs_retry_ds(ngx_http_tfs_t *t)
{
    ngx_http_tfs_inet_t             *addr;
    ngx_http_tfs_segment_data_t     *segment_data;
    ngx_http_tfs_peer_connection_t  *tp;

    tp = t->tfs_peer;
    tp->peer.free(&tp->peer, tp->peer.data, 0);

    segment_data = &t->file.segment_data[t->file.segment_index];
    addr = ngx_http_tfs_select_data_server(t, segment_data);
    if (addr == NULL) {
        switch(t->r_ctx.action.code) {

        case NGX_HTTP_TFS_ACTION_GET_OBJECT:
            t->state = NGX_HTTP_TFS_STATE_READ_GET_BLK_INFO;
            break;

        case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
            if (t->is_large_file && t->is_process_meta_seg) {
                return NGX_HTTP_TFS_EIXT_SERVER_OBJECT_NOT_FOUND;
            }
            // TODO: dedup
            return NGX_ERROR;

        case NGX_HTTP_TFS_ACTION_PUT_OBJECT:
            /* stat retry_ds failed, do not dedup, save new tfs file and do not save tair */
            if (t->is_stat_dup_file) {
                t->is_stat_dup_file = NGX_HTTP_TFS_NO;
                t->use_dedup = NGX_HTTP_TFS_NO;
                t->state = NGX_HTTP_TFS_STATE_WRITE_GET_CLUSTER_ID;
                t->file.segment_data[0].segment_info.block_id = 0;
                t->file.segment_data[0].segment_info.file_id = 0;
                t->retry_curr_ns = NGX_HTTP_TFS_YES;
                break;
            }
        default:
            return NGX_ERROR;
        }

        t->tfs_peer = ngx_http_tfs_select_peer(t);
        if (t->tfs_peer == NULL) {
            return NGX_ERROR;
        }

        t->recv_chain->buf = &t->header_buffer;
        t->recv_chain->next->buf = &t->tfs_peer->body_buffer;

        /* reset ds retry count */
        segment_data->ds_retry = 0;

        if (t->retry_handler == NULL) {
            return NGX_ERROR;
        }

        return t->retry_handler(t);
    }

    ngx_http_tfs_peer_set_addr(t->pool,
        &t->tfs_peer_servers[NGX_HTTP_TFS_DATA_SERVER], addr);

    if (ngx_http_tfs_reinit(t->data, t) != NGX_OK) {
        return NGX_ERROR;
    }

    ngx_http_tfs_connect(t);

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ds_read(ngx_http_tfs_t *t)
{
    size_t                           size;
    ngx_int_t                        rc;
    ngx_buf_t                       *b;
    ngx_peer_connection_t           *p;
    ngx_http_tfs_segment_data_t     *segment_data;
    ngx_http_tfs_peer_connection_t  *tp;
    ngx_http_tfs_logical_cluster_t  *logical_cluster;

    tp = t->tfs_peer;
    p = &tp->peer;
    b = &tp->body_buffer;

    size = ngx_buf_size(b);
    if (size == 0) {
        ngx_log_error(NGX_LOG_INFO, t->log, 0, "process ds read is zero");
        return NGX_AGAIN;
    }

    rc = ngx_http_tfs_data_server_parse_message(t);
    if (rc == NGX_ERROR || rc == NGX_HTTP_TFS_AGAIN) {
        ngx_http_tfs_clear_buf(b);
        return rc;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, t->log, 0, "t->length is %O, rc is %i",
                   t->length, rc);

    b->pos += size;

    if (t->length > 0) {
        return NGX_AGAIN;
    }

    segment_data = &t->file.segment_data[t->file.segment_index];

    switch (t->r_ctx.action.code) {
    case NGX_HTTP_TFS_ACTION_GET_OBJECT:
        if (t->length == 0) {
            t->file.left_length -= segment_data->oper_size;
            t->file.file_offset += segment_data->oper_size;

            if (t->file.left_length == 0) {
                /* large_file meta segment */
                if (t->is_large_file && t->is_process_meta_seg) {
                    /* ready to read data segments */
                    *(t->meta_segment_data->buf) = *b;
                    /* reset buf pos to get whole file data */
                    t->meta_segment_data->buf->pos = t->meta_segment_data->buf->start;
                    rc = ngx_http_tfs_get_segment_for_read(t);
                    if (rc == NGX_ERROR) {
                        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                      "get segment for read failed");
                        return NGX_ERROR;
                    }

                    if (rc == NGX_DONE) { /* pread and start_offset > file size */
                        t->state = NGX_HTTP_TFS_STATE_READ_DONE;
                        t->file_name = t->r_ctx.file_path_s;

                        return NGX_DONE;
                    }

                    t->is_process_meta_seg = NGX_HTTP_TFS_NO;
                    /* later will be alloc */
                    ngx_memzero(&t->tfs_peer->body_buffer, sizeof(ngx_buf_t));

                    t->state = NGX_HTTP_TFS_STATE_READ_GET_BLK_INFO;

                    t->block_cache_ctx.curr_lookup_cache = NGX_HTTP_TFS_LOCAL_BLOCK_CACHE;
                    t->decline_handler = ngx_http_tfs_batch_lookup_block_cache;
                    return NGX_DECLINED;
                }

                /* sub process also return here */
                t->state = NGX_HTTP_TFS_STATE_READ_DONE;
                t->file_name = t->r_ctx.file_path_s;

                return NGX_DONE;
            }

            /* small file */
            if ((t->r_ctx.version == 1 && !t->is_large_file)
                || (t->is_large_file && t->is_process_meta_seg))
            {
                segment_data->oper_size = ngx_min(t->file.left_length,
                                                  NGX_HTTP_TFS_MAX_READ_FILE_SIZE);
                segment_data->oper_offset = t->file.file_offset;
                return rc;
            }
        }
        break;
    case NGX_HTTP_TFS_ACTION_DEL_OBJECT:
        /* NGX_HTTP_TFS_STATE_REMOVE_READ_META_SEGMENT */
        if (t->length == 0) {
            t->file.left_length -= segment_data->oper_size;

            if (t->file.left_length == 0) {
                if (!t->is_large_file && t->use_dedup) {
                    logical_cluster = &t->rc_info_node->logical_clusters[t->logical_cluster_index];
                    rc = ngx_http_tfs_get_dedup_instance(&t->dedup_ctx,
                             &logical_cluster->dup_server_info,
                             logical_cluster->dup_server_addr_hash);

                    if (rc == NGX_ERROR) {
                        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                      "get dedup instance failed.");
                        /* get dedup instance fail, do not unlink file, return success */
                        t->state = NGX_HTTP_TFS_STATE_REMOVE_DONE;
                        return NGX_DONE;
                    }
                    *(t->meta_segment_data->buf) = t->tfs_peer->body_buffer;
                    /* reset buf pos to get whole file data */
                    t->meta_segment_data->buf->pos = t->meta_segment_data->buf->start;
                    t->dedup_ctx.file_data = t->meta_segment_data;
                    t->decline_handler = ngx_http_tfs_get_duplicate_info;
                    return NGX_DECLINED;
                }
                if (t->is_large_file) {
                    *(t->meta_segment_data->buf) = t->tfs_peer->body_buffer;
                    /* reset buf pos to get whole file data */
                    t->meta_segment_data->buf->pos = t->meta_segment_data->buf->start;
                    rc = ngx_http_tfs_get_segment_for_delete(t);
                    if (rc == NGX_ERROR) {
                        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                                      "get segment for delete failed");
                        return NGX_ERROR;
                    }
                    t->is_process_meta_seg = NGX_HTTP_TFS_NO;
                    /* later will be alloc */
                    ngx_memzero(&t->tfs_peer->body_buffer, sizeof(ngx_buf_t));
                    t->state = NGX_HTTP_TFS_STATE_REMOVE_DELETE_DATA;
                }

            } else {
                t->file.file_offset += segment_data->oper_size;
                segment_data->oper_size = ngx_min(t->file.left_length,
                                              NGX_HTTP_TFS_MAX_READ_FILE_SIZE);
                segment_data->oper_offset = t->file.file_offset;
            }
        }
        break;
    }

    return rc;
}


ngx_int_t
ngx_http_tfs_process_ds_input_filter(ngx_http_tfs_t *t)
{
    uint16_t                          msg_type;
    uint32_t                          body_len;
    ngx_http_tfs_segment_data_t      *segment_data;
    ngx_http_tfs_ds_read_response_t  *resp;

    resp = (ngx_http_tfs_ds_read_response_t *) t->header;
    msg_type = resp->header.type;
    if (msg_type == NGX_HTTP_TFS_STATUS_MESSAGE) {
        t->length = resp->header.len - sizeof(uint32_t);
        return NGX_OK;
    }

    segment_data = &t->file.segment_data[t->file.segment_index];
    if (resp->data_len < 0) {
        if (resp->data_len == NGX_HTTP_TFS_EXIT_NO_LOGICBLOCK_ERROR) {
            ngx_http_tfs_remove_block_cache(t, segment_data);

        } else if (resp->data_len == -22) {
            /* for compatibility, old dataserver will return this instead of -1007 */
            resp->data_len = NGX_HTTP_TFS_EXIT_INVALID_ARGU_ERROR;
        }

        /* must be bad request, do not retry */
        if (resp->data_len == NGX_HTTP_TFS_EXIT_READ_OFFSET_ERROR
            || resp->data_len == NGX_HTTP_TFS_EXIT_INVALID_ARGU_ERROR
            || resp->data_len == NGX_HTTP_TFS_EXIT_PHYSIC_BLOCK_OFFSET_ERROR)
        {
            return resp->data_len;
        }
        ngx_log_error(NGX_LOG_ERR, t->log, 0,
                      "read file(block id: %uD, file id: %uL) from (%s) fail, "
                      "error code: %D, will retry",
                      segment_data->segment_info.block_id,
                      segment_data->segment_info.file_id,
                      t->tfs_peer->peer_addr_text, resp->data_len);
        return NGX_HTTP_TFS_AGAIN;
    }

    if (resp->data_len == 0) {
        t->state = NGX_HTTP_TFS_STATE_READ_DONE;
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, t->log, 0, "read len is 0");
        return NGX_DONE;
    }

    body_len = resp->header.len - sizeof(uint32_t);
    t->length = body_len;
    /* in readv2, body_len = resp->data_len + 40 */
    segment_data->oper_size = resp->data_len;
    /* sub process only read once */
    if (t->parent) {
        t->file.left_length = resp->data_len;
    }

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, t->log, 0,
                   "read len is %O, data len is %D",
                   t->length, resp->data_len);

    return NGX_OK;
}


ngx_int_t
ngx_http_tfs_process_ms_input_filter(ngx_http_tfs_t *t)
{
    size_t                           size;
    uint16_t                         msg_type;
    ngx_buf_t                       *b;
    ngx_http_tfs_header_t           *header;
    ngx_http_tfs_peer_connection_t  *tp;

    header = (ngx_http_tfs_header_t *) t->header;
    t->length = header->len;

    msg_type = header->type;
    if (msg_type == NGX_HTTP_TFS_STATUS_MESSAGE) {
        return NGX_OK;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, t->log, 0, "get bucket rsp len is %O",
                   t->length);

    /* modify body buffer size so that it can hold all data, then we do parse */
    tp = t->tfs_peer;
    b = &tp->body_buffer;
    size = b->end - b->start;
    if (size < (size_t)t->length) {
        b->start = ngx_prealloc(t->pool, b->start, size, t->length);
        if (b->start == NULL) {
            return NGX_ERROR;
        }
        b->end = b->last + t->length;
    }

    b->pos = b->start;
    b->last = b->start;

    return NGX_OK;
}


