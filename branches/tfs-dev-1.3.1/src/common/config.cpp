/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *      - initial release
 *
 */
#include "config.h"
#include "config_item.h"
#include "error_msg.h"

using namespace std;
namespace tfs
{
  namespace common
  {
    static Configure configure;
    static char g_cluster_id = '1';

    Configure::Configure()
    {
      public_config_map_[CONF_BLOCK_MAX_SIZE] = "67108864"; // 64M
      public_config_map_[CONF_USE_CAPACITY_RATIO] = "80"; // 80%
      public_config_map_[CONF_MIN_REPLICATION] = "3"; // min replic
      public_config_map_[CONF_MAX_REPLICATION] = "5"; // max replic

      name_config_map_[CONF_PORT] = "3100";
      name_config_map_[CONF_THREAD_COUNT] = "10";
      name_config_map_[CONF_LOG_FILE] = "nameserver.log";
      name_config_map_[CONF_LOCK_FILE] = "nameserver.pid";
      is_load_ = false;
      config_file_name_ = "";
    }

    Configure::~Configure()
    {
    }

    Configure& Configure::get_configure()
    {
      configure.check_load();
      return configure;
    }

    int32_t Configure::get_cluster_id()
    {
      configure.check_load();
      return g_cluster_id;
    }

    void Configure::set_cluster_id(const int32_t cluster_id)
    {
      g_cluster_id = cluster_id;
    }

    int Configure::parse_value(char* str, char* key, char* val)
    {
      char *p, *p1, *name, *value;

      if (str == NULL)
        return -1;

      p = str;
      while ((*p) == ' ' || (*p) == '\t' || (*p) == '\r' || (*p) == '\n')
        p++;
      p1 = p + strlen(p);
      while ((*(p1 - 1)) == ' ' || (*(p1 - 1)) == '\t' || (*(p1 - 1)) == '\r' || (*(p1 - 1)) == '\n')
        p1--;
      (*p1) = '\0';
      if (*p == '#' || *p == '\0')
        return -1;
      p1 = strchr(str, '=');
      if (p1 == NULL)
        return -2;
      name = p;
      value = p1 + 1;
      while ((*(p1 - 1)) == ' ')
        p1--;
      (*p1) = '\0';

      while ((*value) == ' ')
        value++;
      p = strchr(value, '#');
      if (p == NULL)
        p = value + strlen(value);
      while ((*(p - 1)) <= ' ')
        p--;
      (*p) = '\0';
      if (name[0] == '\0')
        return -2;

      strcpy(key, name);
      strcpy(val, value);

      return TFS_SUCCESS;
    }

    int Configure::load(const std::string& filename)
    {
      FILE *fp;
      char key[CFG_KEY_LEN], value[CFG_BUFFER_LEN], data[CFG_BUFFER_LEN];
      int ret, line = 0;

      if ((fp = fopen(filename.c_str(), "rb")) == NULL)
      {
        fprintf(stderr, "cant open config file '%s'\n", filename.c_str());
        is_load_ = true;
        return EXIT_OPEN_FILE_ERROR;
      }
      clear_config_map();
      STRING_MAP *m = &public_config_map_;
      while (fgets(data, 4096, fp))
      {
        line++;
        if (strncmp(CONF_SN_PUBLIC, data, strlen(CONF_SN_PUBLIC)) == 0)
        {
          m = &public_config_map_;
          continue;
        }
        else if (strncmp(CONF_SN_NAMESERVER, data, strlen(CONF_SN_NAMESERVER)) == 0)
        {
          m = &name_config_map_;
          continue;
        }
        else if (strncmp(CONF_SN_DATASERVER, data, strlen(CONF_SN_DATASERVER)) == 0)
        {
          m = &data_config_map_;
          continue;
        }
        else if (strncmp(CONF_SN_TFSCLIENT, data, strlen(CONF_SN_TFSCLIENT)) == 0)
        {
          m = &client_config_map_;
          continue;
        }
        else if (strncmp(CONF_SN_ADMINSERVER, data, strlen(CONF_SN_ADMINSERVER)) == 0)
        {
          m = &admin_config_map_;
          continue;
        }
        ret = parse_value(data, key, value);
        if (ret == -2)
        {
          fprintf(stderr, "parse error: Line: %d, %s\n", line, data);
          return EXIT_GENERAL_ERROR;
        }
        if (ret < 0)
        {
          continue;
        }
        if (m->find(key) != m->end())
        {
          m->erase(key);
        }
        m->insert(STRING_MAP::value_type(Func::str_to_lower(key), value));
      }
      fclose(fp);
      char* tarea = get_string_value(CONFIG_PUBLIC, CONF_CLUSTER_ID, "1");
      g_cluster_id = tarea[0];
      is_load_ = true;
      config_file_name_ = filename;
      return TFS_SUCCESS;
    }

    void Configure::clear_config_map()
    {
      public_config_map_.clear();
      name_config_map_.clear();
      data_config_map_.clear();
      client_config_map_.clear();
      admin_config_map_.clear();
    }

    int Configure::check_load()
    {
      if (!is_load_)
      {
        char *f = getenv("TFS_CONF_FILE");
        if (f != NULL)
        {
          load(f);
        }
        is_load_ = true;
      }
      return TFS_SUCCESS;
    }

    char* Configure::get_string_value(const int section, const std::string& key, char *d)
    {
      STRING_MAP *m;
      switch (section)
      {
      case CONFIG_PUBLIC:
        m = &public_config_map_;
        break;
      case CONFIG_NAMESERVER:
        m = &name_config_map_;
        break;
      case CONFIG_DATASERVER:
        m = &data_config_map_;
        break;
      case CONFIG_TFSCLIENT:
        m = &client_config_map_;
        break;
      case CONFIG_ADMINSERVER:
        m = &admin_config_map_;
        break;
      default:
        return d;
        break;
      }
      STRING_MAP_ITER it = m->find(key);
      if (it != m->end())
      {
        return (char *) it->second.c_str();
      }
      else
      {
        return d;
      }
    }

    int Configure::get_int_value(const int section, const std::string& key, const int d)
    {
      char* str = get_string_value(section, key);
      if (str == NULL || (*str) == '\0')
        return d;
      char *p = str;
      while ((*p))
      {
        if ((*p) < '0' || (*p) > '9')
          return d;
        p++;
      }
      return atoi(str);
    }

    string Configure::to_string()
    {
      STRING_MAP_ITER it;
      string result = CONF_SN_PUBLIC;
      result += "\n";
      for (it = public_config_map_.begin(); it != public_config_map_.end(); it++)
      {
        result += "    " + it->first + " = " + it->second + "\n";
      }
      result += "\n";
      result += CONF_SN_NAMESERVER;
      result += "\n";
      for (it = name_config_map_.begin(); it != name_config_map_.end(); ++it)
      {
        result += "    " + it->first + " = " + it->second + "\n";
      }
      result += "\n";
      result += CONF_SN_DATASERVER;
      result += "\n";
      for (it = data_config_map_.begin(); it != data_config_map_.end(); ++it)
      {
        result += "    " + it->first + " = " + it->second + "\n";
      }
      result += "\n";
      result += CONF_SN_TFSCLIENT;
      result += "\n";
      for (it = client_config_map_.begin(); it != client_config_map_.end(); ++it)
      {
        result += "    " + it->first + " = " + it->second + "\n";
      }
      result += "\n";
      result += CONF_SN_ADMINSERVER;
      result += "\n";
      for (it = admin_config_map_.begin(); it != admin_config_map_.end(); ++it)
      {
        result += "    " + it->first + " = " + it->second + "\n";
      }
      result += "\n";
      return result;
    }

    STRING_MAP Configure::get_define_key()
    {
      STRING_MAP smap;
      STRING_MAP_ITER it;
      for (it = public_config_map_.begin(); it != public_config_map_.end(); ++it)
      {
        smap.insert(STRING_MAP::value_type(it->first, "1"));
      }
      for (it = name_config_map_.begin(); it != name_config_map_.end(); ++it)
      {
        smap.insert(STRING_MAP::value_type(it->first, "1"));
      }
      for (it = data_config_map_.begin(); it != data_config_map_.end(); ++it)
      {
        smap.insert(STRING_MAP::value_type(it->first, "1"));
      }
      for (it = client_config_map_.begin(); it != client_config_map_.end(); ++it)
      {
        smap.insert(STRING_MAP::value_type(it->first, "1"));
      }
      for (it = admin_config_map_.begin(); it != admin_config_map_.end(); ++it)
      {
        smap.insert(STRING_MAP::value_type(it->first, "1"));
      }
      return smap;
    }

  }
}
