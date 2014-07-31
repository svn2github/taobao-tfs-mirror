/*
 * (C) 2007-2013 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Authors:
 *    gy <ganyu.hfl@taobao.com>
 *     - add easy
 *    linqing <linqing.zyd@taobao.com>
 *     - add easy
 *
 */
#ifndef TFS_COMMON_EASY_HELPER_H
#define TFS_COMMON_EASY_HELPER_H

#define EASY_MULTIPLICITY

#include <easy_io.h>
#include <sys/prctl.h>
#include <string>
namespace tfs
{
  namespace common
  {
    class EasyHelper
    {
      public:
        //~ callback on connect
        static int on_connect_cb(easy_connection_t *c);
        //~ callback on disconnect
        static int on_disconnect_cb(easy_connection_t *c);
        //~ call back on idle, disconnect for the present
        static int on_idle_cb(easy_connection_t *c);
        //~ packet cleaner callback
        static int cleanup_cb(easy_request_t *r, void *);
        //~ buffer cleaner callback
        static void buf_cleanup_cb(easy_buf_t *b, void *args);
        //~ process callback when checking server's aliveness
        static int alive_processor_cb(easy_request_t *r);
        /**
         * Convert between the two formats of server address
         * `idx' is used for choosing IO thread by libeasy if less than 256.
         */
        static easy_addr_t convert_addr(uint64_t server_id, uint32_t idx = 256);
        static uint64_t convert_addr(const easy_addr_t &addr);
        //~ check if one server is alive
        static bool is_alive(easy_addr_t &addr, easy_io_t *eio = NULL);
        static bool is_alive(uint64_t server_id, easy_io_t *eio = NULL);
        //~ initialize a easy_io_handler
        static void init_handler(easy_io_handler_pt *handler, void *user_data);
        static int easy_async_send(easy_io_t *eio,
            uint64_t server_id,
            void *packet,
            void *args,
            easy_io_handler_pt *handler,
            int timeout = DEFAULT_NETWORK_CALL_TIMEOUT,
            int retry = 0);
        static int easy_async_send(easy_io_t *eio,
            const easy_addr_t &addr,
            void *packet,
            void *args,
            easy_io_handler_pt *handler,
            int timeout = DEFAULT_NETWORK_CALL_TIMEOUT,
            int retry = 0);
        static void* easy_sync_send(easy_io_t *eio,
            uint64_t server_id,
            void *packet,
            void *args,
            easy_io_handler_pt *handler,
            int timeout = DEFAULT_NETWORK_CALL_TIMEOUT,
            int retry = 0);
        static void* easy_sync_send(easy_io_t *eio,
            const easy_addr_t &addr,
            void *packet,
            void *args,
            easy_io_handler_pt *handler,
            int timeout = DEFAULT_NETWORK_CALL_TIMEOUT,
            int retry = 0);

        template <typename T> //~ each group of ioth in T::eio base on the same index
          static void easy_set_thread_name(void *prefix) {
            static int idx = 0;
            char ioth_name[16];
            snprintf(ioth_name, sizeof(ioth_name), "%s%02d", static_cast<const char*> (prefix), __sync_fetch_and_add(&idx, 1));
            prctl(PR_SET_NAME, ioth_name, 0, 0, 0);
          }

        static void set_thread_name(const char *name) {
          prctl(PR_SET_NAME, name, 0, 0, 0);
        }
        static void easy_set_log_level(int tbsys_level);
        static std::string easy_connection_to_str(easy_connection_t *c) {
          char buf[32];
          easy_inet_addr_to_str(&c->addr, buf, sizeof(buf));
          return std::string(buf);
        }

      private:
        static bool wait_connected(int fd, int timeout/*ms*/);

      private:
        static easy_io_t *eio;
        static easy_io_handler_pt *handler;
    };
  }
}
#endif
