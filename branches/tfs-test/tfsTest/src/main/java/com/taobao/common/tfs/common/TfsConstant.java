/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.common;

import java.io.File;
import java.nio.ByteOrder;

public class TfsConstant {
    /** client version */
    public static final String CLIENT_VERSION = "TFS-java-V2.1.4";

    /** base constant */
    public static final int BYTE_SIZE = 1;
    public static final int SHORT_SIZE = Short.SIZE / 8;
    public static final int INT_SIZE = Integer.SIZE / 8;
    public static final int LONG_SIZE = Long.SIZE / 8;

    /** packet flag */
    public static final int TFS_PACKET_FLAG = 0x4e534654;

    /** timeout */
    public static final int DEFAULT_TIMEOUT = 2000;

    /** exit status value */
    public static final int TFS_SUCCESS = 0;
    public static final int TFS_ERROR = 1;
    public static final int EXIT_GENERAL_ERROR = -1000;
    public static final int EXIT_ALL_SEGMENT_FAIL_ERROR = -1004;
    public static final int EXIT_INVALIDFD_ERROR = -1005;
    public static final int EXIT_NO_LOGICBLOCK_ERROR = -8006;
    public static final int EXIT_TABLE_VERSION_ERROR = -15003;
    
    /** segment */
    public static final int MAX_BATCH_COUNT = 8;
    public static final int MAX_SEGMENT_LENGTH = 1 << 21;   // 2M
    public static final int MIN_GC_EXPIRED_TIME = 86400000; // 1day

    /** temporary path */
    public static final char SEPARATOR = File.separatorChar;
    public static final String SYS_TMP_PATH = System.getProperty("java.io.tmpdir");
    // double file.separatorChar is not recognized by Windows... check it
    public static final String TFS_TMP_PATH =
        (SYS_TMP_PATH.endsWith("" + SEPARATOR) ? SYS_TMP_PATH : SYS_TMP_PATH + SEPARATOR) +
        "TFSlocalkeyDIR" +
        SEPARATOR;
    public static final String GC_FILE_PATH = TFS_TMP_PATH + "gc" + SEPARATOR;
    // just consider os exclude windows
    public static final boolean TFS_TMP_PATH_NEED_CHMOD =
        ! System.getProperty("os.name").toLowerCase().matches(".*window.*");
    public static final String TFS_TMP_PATH_MODE = " 0777 ";
    public static final String CHMOD_CMD = "chmod ";

    /** open mode */
    /** client */
    public static final int READ_MODE = 0x1;
    public static final int WRITE_MODE = 0x2;
    public static final int CREATEBLK_MODE = 0x4;
    public static final int NEWBLK_MODE = 0x8;
    public static final int NOLEASE_MODE = 0x10;
    public static final int STAT_MODE = 0x20;
    public static final int UNLINK_MODE = 0x40;

    /** file status */
    public static final int NORMAL_STATUS = 0x0;
    public static final int UNLINK_STATUS = 0x1;
    public static final int HIDE_STATUS = 0x4;

    /** message packet type code */
    public static final int STATUS_MESSAGE  = 1;
    public static final int GET_BLOCK_INFO_MESSAGE = 2;
    public static final int SET_BLOCK_INFO_MESSAGE = 3;
    public static final int CARRY_BLOCK_MESSAGE = 4;
    public static final int SET_DATASERVER_MESSAGE = 5;
    public static final int UPDATE_BLOCK_INFO_MESSAGE = 6;
    public static final int READ_DATA_MESSAGE = 7;
    public static final int RESP_READ_DATA_MESSAGE = 8;
    public static final int WRITE_DATA_MESSAGE = 9;
    public static final int CLOSE_FILE_MESSAGE = 10;
    public static final int UNLINK_FILE_MESSAGE = 11;
    public static final int REPLICATE_BLOCK_MESSAGE = 12;
    public static final int COMPACT_BLOCK_MESSAGE = 13;
    public static final int GET_SERVER_STATUS_MESSAGE = 14;
    public static final int SET_SERVER_STATUS_MESSAGE = 15;
    public static final int SUSPECT_DATASERVER_MESSAGE = 16;
    public static final int FILE_INFO_MESSAGE = 17;
    public static final int RESP_FILE_INFO_MESSAGE = 18;
    public static final int RENAME_FILE_MESSAGE = 19;
    public static final int CLIENT_CMD_MESSAGE = 20;
    public static final int CREATE_FILENAME_MESSAGE = 21;
    public static final int RESP_CREATE_FILENAME_MESSAGE = 22;
    public static final int ROLLBACK_MESSAGE = 23;
    public static final int RESP_HEART_MESSAGE = 24;
    public static final int RESET_BLOCK_VERSION_MESSAGE = 25;
    public static final int BLOCK_FILE_INFO_MESSAGE = 26;
    public static final int UNIQUE_FILE_MESSAGE = 27;
    public static final int RETRY_COMMAND_MESSAGE = 28;
    public static final int NEW_BLOCK_MESSAGE = 29;
    public static final int REMOVE_BLOCK_MESSAGE = 30;
    public static final int LIST_BLOCK_MESSAGE = 31;
    public static final int RESP_LIST_BLOCK_MESSAGE = 32;
    public static final int BLOCK_WRITE_COMPLETE_MESSAGE = 33;
    public static final int BLOCK_INODE_INFO_MESSAGE = 34;
    public static final int WRITE_DATA_BATCH_MESSAGE = 35;
    public static final int WRITE_INFO_BATCH_MESSAGE = 36;
    public static final int BLOCK_COMPACT_COMPLETE_MESSAGE = 37;
    public static final int READ_DATA_MESSAGE_V2 = 38;
    public static final int RESP_READ_DATA_MESSAGE_V2 = 39;
    public static final int LIST_BITMAP_MESSAGE = 40;
    public static final int RESP_LIST_BITMAP_MESSAGE = 41;
    public static final int RELOAD_CONFIG_MESSAGE = 42;
    public static final int BATCH_GET_BLOCK_INFO_MESSAGE = 59;
    public static final int BATCH_SET_BLOCK_INFO_MESSAGE = 60;
    // rc packet
    public static final int REQ_RC_LOGIN_MESSAGE = 66;
    public static final int RESP_RC_LOGIN_MESSAGE = 67;
    public static final int REQ_RC_KEEPALIVE_MESSAGE = 68;
    public static final int RESP_RC_KEEPALIVE_MESSAGE = 69;
    public static final int REQ_RC_LOGOUT_MESSAGE = 70;
    public static final int REQ_RC_RELOAD_MESSAGE = 71;

    // name meta packet
    public static final int NAME_META_FILE_ACTION_MESSAGE = 74;
    public static final int NAME_META_WRITE_MESSAGE = 75;
    public static final int NAME_META_READ_MESSAGE = 76;
    public static final int RESP_NAME_META_READ_MESSAGE = 77;
    public static final int NAME_META_LS_MESSAGE = 78;
    public static final int RESP_NAME_META_LS_MESSAGE = 79;
    public static final int NAME_META_GET_TABLE_MESSAGE = 84;
    public static final int RESP_NAME_META_GET_TABLE_MESSAGE = 85;
    
    /** max lengh of waiting task queue */
    public static final int WAIT_QUEUE_LENGTH = 1024;

    /** max size of memory allocated */
    public static final int TFS_MALLOC_MAX = 1 << 20; //  1MB

    /** byteorder of protocal */
    public static final ByteOrder TFS_PROTOCOL_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /** unique store magic version when first inserting to tair */
    public static final int FIRST_INSERT_UNIQUE_MAGIC_VERSION = 0x0fffffff;

    /** unique store tair return value */
    public static final int UNIQUE_INSERT_VERSION_ERROR = -1;
    public static final int UNIQUE_QUERY_NOT_EXIST = -2;
    
    /** name meta table */
    public static final long MAX_BUCKET_ITEM_DEFAULT = 1024;
    public static final long MAX_SERVER_ITEM_DEFAULT = 1024;
    public static final long MAX_BUCKET_DATA_LENGTH = MAX_BUCKET_ITEM_DEFAULT * LONG_SIZE;
}

