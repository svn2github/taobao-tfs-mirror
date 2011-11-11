package com.taobao.common.tfs.function;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.ArrayList;

import com.taobao.gaia.AppCluster;
import com.taobao.gaia.AppGrid;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.HelpConf;
import com.taobao.gaia.HelpFile;
import com.taobao.gaia.HelpHA;
import com.taobao.gaia.HelpLog;
import com.taobao.gaia.HelpProc;
import com.taobao.gaia.KillTypeEnum;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class NameMetaBaseCase extends TfsBaseCase{

    final static ClassPathXmlApplicationContext serverFactory = new ClassPathXmlApplicationContext("nameMetaServer.xml");
    final static AppGrid nameMetaGrid = (AppGrid)serverFactory.getBean("nameMetaGrid");
    final static ClassPathXmlApplicationContext clientFactory = new ClassPathXmlApplicationContext("nameMetaClient.xml");
    final static AppServer nameMetaClient = (AppServer)clientFactory.getBean("nameMetaClient");

    // Define
    // server related
    final public static int RSINDEX = 0;
    final public static int MSINDEX = 1;
    final public int META_COUNT = 3;
    final public int RS_MAX_COUNT = 2;
    final public String RSVIP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getVip();
    public String MASTER_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getIp();
    //final public String SLAVE_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(1).getIp();
    final public String RS_LOG = nameMetaGrid.getCluster(RSINDEX).getServer(0).getLogs();
    public String SLAVE_RS_IP = "";
    final public int RSPORT = nameMetaGrid.getCluster(RSINDEX).getServer(0).getPort();
    final public int MSPORT = nameMetaGrid.getCluster(MSINDEX).getServer(0).getPort();
    final public String MSCONF = nameMetaGrid.getCluster(MSINDEX).getServer(0).getConfname();    
    final public String MS_HOME = nameMetaGrid.getCluster(MSINDEX).getServer(0).getDir();    
    final public String MS_LOG = nameMetaGrid.getCluster(MSINDEX).getServer(0).getLogs();    
    public AppServer MASTER_RS = nameMetaGrid.getCluster(RSINDEX).getServer(0);
    //public AppServer SLAVE_RS = nameMetaGrid.getCluster(RSINDEX).getServer(1);
    public AppServer SLAVE_RS = null;

    // ha related
    final public String VIP_ETH_NAME = "eth0:1";

    // client related
    final public static String CLIENT_CONF = nameMetaClient.getConfname();    

    // operation type
    public int OPER_CREATE_DIR = 1;
    public int OPER_LS_DIR = 2;
    public int OPER_MV_DIR = 4;
    public int OPER_RM_DIR = 8;
    public int OPER_CREATE_FILE = 16;
    public int OPER_LS_FILE = 32;
    public int OPER_SAVE_SMALL_FILE = 64;
    public int OPER_SAVE_LARGE_FILE = 128;
    public int OPER_FETCH_FILE = 256;
    public int OPER_READ = 512;
    public int OPER_WRITE = 1024;
    public int OPER_MV_FILE = 2048;
    public int OPER_RM_FILE = 4096;
    public int OPER_MIX = 8192;

    public String STR_OPER_CREATE_DIR = "CreateDir";
    public String STR_OPER_LS_DIR = "LsDir";
    public String STR_OPER_MV_DIR = "MvDir";
    public String STR_OPER_RM_DIR = "RmDir";
    public String STR_OPER_CREATE_FILE = "CreateFile";
    public String STR_OPER_LS_FILE = "LsFile";
    public String STR_OPER_SAVE_SMALL_FILE = "SavaSmallFile";
    public String STR_OPER_SAVE_LARGE_FILE = "SaveLargeFile";
    public String STR_OPER_FETCH_FILE = "FetchFile";
    public String STR_OPER_MV_FILE = "MvFile";
    public String STR_OPER_RM_FILE = "RmFile";
    public String STR_OPER_MIX = "Mix";
 
 
    final public static String CLIENT_IP = nameMetaClient.getIp();
    final public static String CLIENT_HOME = nameMetaClient.getDir();
    final public static String CLIENT_LOG = nameMetaClient.getLogs();
    final public String MYSQL_SCRIPT = "do_mysql.sh";
    final public String PERF_SCRIPT = "gen_perf_report.sh";
    final public String START_CLIENT_CMD = "/bin/bash meta_oper.sh start_oper " + CLIENT_CONF;
    final public String STOP_OPER_CMD = "/bin/bash meta_oper.sh stop_oper";
    final public String GEN_DIR_TREE_CMD = "/bin/bash gen_dir_tree.sh";

    final public String SAVED_FILE_LIST = "oper_save_file.fileList.";
    final public String CREATED_DIR_LIST = "oper_create_dir.fileList.";

    // time
    final public int LEASE_TIME = 6;
    final public int SCAN_TIME = 15;

    // keywords
    final public String KW_SERVING_MS_IP = "to metaServer";
    final public String KW_APP_ID = "appId: ";
    final public String KW_USER_ID = "userId: ";
    final public String KW_LS_DIR_STATIS = "oper_ls_dir stat info";
    final public String KW_LS_FILE_STATIS = "oper_ls_file stat info";
    final public String KW_SAVE_SMALL_FILE_STATIS = "oper_save_small_file stat info";
    final public String KW_FETCH_FILE_STATIS = "oper_fetch_file stat info";
    final public String KW_CREATE_DIR_STATIS = "oper_create_dir stat info";

    final public String KW_CACHE_SIZE = "malloc size";
    final public String KW_CACHE_GC = "gc app_id ";

    // columns
    final public int COL_MS_IP = 12;
    final public int COL_TAIL_RATE = 15;
    final public int COL_CACHE_SIZE = 13;
    final public int COL_USED_SIZE = 17;
    final public int COL_GC_USER = 10;

    // metaServer config items
    final public String CONF_LOG_LEVEL = "log_level";
    final public String CONF_MAX_CACHE_SIZE = "max_cache_size";
    final public String CONF_MAX_SPOOL_SIZE = "max_spool_size";
    final public String CONF_MAX_MUTEX_SIZE = "max_mutex_size";
    final public String CONF_FREE_LIST_COUNT = "free_list_count";
    final public String CONF_GC_RATIO = "gc_ratio";
    final public String CONF_GC_INTERVAL = "gc_interval";
    final public String CONF_MAX_SUB_DIRS_COUNT = "max_sub_dirs_count";
    final public String CONF_MAX_SUB_DIRS_DEEP = "max_sub_dirs_deep";
    final public String CONF_MAX_SUB_FILES_COUNT = "max_sub_files_count";

    // rootServer config items
    final public String CONF_MTS_RTS_LEASE_EXPIRED_TIME = "mts_rts_lease_expired_time";
    final public String CONF_MTS_RTS_LEASE_INTERVAL = "mts_rts_lease_interval";
    final public String CONF_RTS_RTS_LEASE_EXPIRED_TIME = "rts_rts_lease_expired_time";
    final public String CONF_RTS_RTS_LEASE_INTERVAL = "rts_rts_lease_interval";

    // client config items
    final public String CONF_CLIENT_PUBLIC = "public";
    final static String CONF_CREATE_DIR = "CreateDir";
    final static String CONF_LS_DIR = "LsDir";
    final static String CONF_MV_DIR = "MvDir";
    final static String CONF_RM_DIR = "RmDir";
    final static String CONF_CREATE_FILE = "CreateFile";
    final static String CONF_SAVE_SMALL_FILE = "SaveSmallFile";
    final static String CONF_SAVE_LARGE_FILE = "SaveLargeFile";
    final static String CONF_FETCH_FILE = "FetchFile";
    final static String CONF_LS_FILE = "LsFile";
    final static String CONF_MV_FILE = "MvFile";
    final static String CONF_RM_FILE = "RmFile";
    final static String CONF_MIX = "Mix";

    final public String CONF_CLIENT_OPER_TYPE = "operType";
    final public String CONF_CLIENT_USER_ID = "userId";
    final public String CONF_CLIENT_THREAD_COUNT = "threadCount";
    final public String CONF_CLIENT_AUTO_GEN_DIR = "autoGenDir";

    // limits
    final public int LIMIT_MAX_SPOOL_SIZE = 20;

    // other
    final public int TAIL_LINE = 1000;

    @BeforeClass
    public  static void setUpOnce() throws Exception {
      boolean bRet = false;
      /* Kill the grid */
      bRet = nameMetaGrid.stop(KillTypeEnum.FORCEKILL, WAIT_TIME);
      Assert.assertTrue(bRet);

      /* Clean the log file */
      bRet = nameMetaGrid.clean();
      Assert.assertTrue(bRet);

      bRet = nameMetaGrid.start();
      Assert.assertTrue(bRet);
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
    }

    // fuctions

    // set metaServer conf
    public boolean setLogLevel(String metaServerIp, String logLevel) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "public", CONF_LOG_LEVEL, logLevel);
        return bRet;
    }

    public boolean setMaxCacheSize(String metaServerIp, int maxCacheSize) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_CACHE_SIZE, String.valueOf(maxCacheSize));
        return bRet;
    }

    public boolean setGcRatio(String metaServerIp, float gcRatio) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_GC_RATIO, String.valueOf(gcRatio));
        return bRet;
    }

    public boolean setMaxSubDirsCount(String metaServerIp, int maxSubDirsCount) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_SUB_DIRS_COUNT, String.valueOf(maxSubDirsCount));
        return bRet;
    }

    public boolean setMaxSubFilesCount(String metaServerIp, int maxSubFilesCount) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_SUB_FILES_COUNT, String.valueOf(maxSubFilesCount));
        return bRet;
    }

    public boolean setMaxSubDirsDeep(String metaServerIp, int maxSubDirsDeep) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_SUB_DIRS_DEEP, String.valueOf(maxSubDirsDeep));
        return bRet;
    }

    public boolean setMaxMutexSize(String metaServerIp, int maxMutexSize) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_MUTEX_SIZE, String.valueOf(maxMutexSize));
        return bRet;
    }

    public boolean setFreeListCount(String metaServerIp, int freeListCount) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_FREE_LIST_COUNT, String.valueOf(freeListCount));
        return bRet;
    }

    public boolean setMaxSpoolSize(String metaServerIp, int maxSpoolSize) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(metaServerIp, MSCONF, "metaserver", CONF_MAX_SPOOL_SIZE, String.valueOf(maxSpoolSize));
        return bRet;
    }

    public boolean setClientOperType(int operType) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(CLIENT_IP, CLIENT_CONF, CONF_CLIENT_PUBLIC, CONF_CLIENT_OPER_TYPE, String.valueOf(operType));
        return bRet;
    }

    public boolean setClientUserId(long userId) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(CLIENT_IP, CLIENT_CONF, CONF_CLIENT_PUBLIC, CONF_CLIENT_USER_ID, String.valueOf(userId));
        return bRet;
    }

    public boolean setClientThreadCount(int threadCount) {
        boolean bRet = false;
        bRet = conf.confReplaceSingleByPart(CLIENT_IP, CLIENT_CONF, CONF_CLIENT_PUBLIC, CONF_CLIENT_THREAD_COUNT, String.valueOf(threadCount));
        return bRet;
    }

    public boolean setClientAutoGenDir(String operConf, boolean autoGenDir) {
        boolean bRet = false;
        String strAutoGenDir = autoGenDir ? "true" : "false";
        bRet = conf.confReplaceSingleByPart(CLIENT_IP, CLIENT_CONF, operConf, CONF_CLIENT_AUTO_GEN_DIR, strAutoGenDir);
        return bRet;
    }

    public String getServingMSIp(long appId, long userId) {
        String metaServerAddr;
        metaServerAddr = getServingMSAddr(appId, userId);
        Assert.assertTrue(metaServerAddr != null);
        log.debug("serving metaServer addr: " + metaServerAddr);

        String [] tmp;
        tmp = metaServerAddr.split(":");
        Assert.assertTrue(tmp.length == 2);
        String metaServerIp = tmp[0];
        log.debug("serving metaServer ip: " + metaServerIp);
        return metaServerIp;
    }

    public String getServingMSAddr(long appId, long userId) {
        boolean bRet = false;
        ArrayList<String> keyWords = new ArrayList<String>();
        keyWords.add(KW_SERVING_MS_IP); 
        keyWords.add(KW_APP_ID + appId); 
        keyWords.add(KW_USER_ID + userId); 
        String logName = CLIENT_HOME + "/log." + caseName;
        ArrayList<String> filter = new ArrayList<String>();
        filter.add("/");
        ArrayList<String> result = new ArrayList<String>();
        String cmd = "tail -1000 " + logName;
        bRet = Proc.cmdOutBase2(CLIENT_IP, cmd, keyWords, COL_MS_IP, filter, result); 
        if (false == bRet || result.size() < 1) return null;
        return result.get(result.size() - 1);
    }

    public String getUnServingMSAddr(long appId, long userId) {
        String retMsAddr = null;
        String targetMsAddr = getServingMSAddr(appId, userId);
        for (int i = 0; i < META_COUNT; i++)
        {
          String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(i).getIp(); 
          int msPort = nameMetaGrid.getCluster(MSINDEX).getServer(i).getPort(); 
          String msAddr = msIp + ":" + msPort;
          log.debug("source: " + targetMsAddr + ", dest: " + msAddr);
          if (! targetMsAddr.equals(msAddr))
          {
            retMsAddr = msAddr;
            break; 
          }
        }
        log.debug("@@@ unservering metaserver: " + retMsAddr);
        return retMsAddr;
    }

    public int getServingIndexFromMS(long appId, long userId) {
        int index = -1;
        boolean bRet = false;
        for (int i = 0; i < META_COUNT; i++)
        {
          String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(i).getIp(); 
          String logName = nameMetaGrid.getCluster(MSINDEX).getServer(i).getLogs(); 
          ArrayList<String> keyWords = new ArrayList<String>();
          keyWords.add(KW_APP_ID_MS + appId); 
          keyWords.add(KW_USER_ID_MS + userId); 
          String cmd = "cat " + logName;
          ArrayList<String> result = new ArrayList<String>();
          bRet = Proc.cmdOutBase2(msIp, cmd, keyWords, 1 , null, result); 
          if (true == bRet && result.size() > 0) {
            index = i;
            break;
          }
        }
        return index;
    }

    public int getServingMSIndex(long appId, long userId) {
        return getMsIndex(getServingMSAddr(appId, userId));
    }

    public int getUnServingMSIndex(long appId, long userId) {
        return getMsIndex(getUnServingMSAddr(appId, userId));
    }

    private int getMsIndex(String targetMsAddr)
    {
        int index = -1;
        log.debug("msIp: " + targetMsAddr);
        for (int i = 0; i < META_COUNT; i++)
        {
          String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(i).getIp(); 
          int msPort = nameMetaGrid.getCluster(MSINDEX).getServer(i).getPort(); 
          String msAddr = msIp + ":" + msPort;
          if (targetMsAddr.equals(msAddr))
          {
            index = i;
            break; 
          }
        }
        return index;
    }

    public boolean createDirCmd(long userId) {
        boolean bRet = false;
        log.debug("Create dir cmd start ===>");
        bRet = setClientOperType(OPER_CREATE_DIR);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Create dir cmd end ===>");
        return bRet;
    }

    public boolean createDirCmdStop() {
        boolean bRet = false;
        log.debug("Create dir cmd stop start ===>");
        bRet = Proc.proStartBase(CLIENT_IP, STOP_OPER_CMD, CLIENT_HOME);
        log.debug("Create dir cmd stop end ===>");
        return bRet;
    }

    // generate a dir tree with specific subdirs and subfiles and deep in a file
    public boolean genDirTree(String metaServerIp, long userId, int maxSubDirsCount, int maxSubFilesCount, int maxSubDirsDeep) {
        boolean bRet = false;
        log.debug("Gen dir tree cmd start ===>");
        String cmd = GEN_DIR_TREE_CMD + " " + userId + " " + maxSubDirsCount + " " + maxSubFilesCount + " " + maxSubDirsDeep;
        bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Gen dir tree cmd end ===>");
        return bRet;
    }

    public boolean createDirMon() {
        boolean bRet = false;
        log.debug("Create dir mon start ===>");
        bRet = operMon();
        log.debug("Create dir mon end ===>");
        return bRet;
    }

    public boolean lsDirCmd(long userId) {
        boolean bRet = false;
        log.debug("Ls dir cmd start ===>");
        bRet = setClientOperType(OPER_LS_DIR);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Ls dir cmd end ===>");
        return bRet;
    }

    public boolean lsDirMon() {
        boolean bRet = false;
        log.debug("Ls dir mon start ===>");
        bRet = operMon();
        log.debug("Ls dir mon end ===>");
        return bRet;
    }

    public boolean mvDirCmd(long userId) {
        boolean bRet = false;
        log.debug("Mv dir cmd start ===>");
        bRet = setClientOperType(OPER_MV_DIR);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Mv dir cmd end ===>");
        return bRet;
    }

    public boolean mvDirMon() {
        boolean bRet = false;
        log.debug("Mv dir mon start ===>");
        bRet = operMon();
        log.debug("Mv dir mon end ===>");
        return bRet;
    }

    public boolean rmDirCmd(long userId) {
        boolean bRet = false;
        log.debug("Rm dir cmd start ===>");
        bRet = setClientOperType(OPER_RM_DIR);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Rm dir cmd end ===>");
        return bRet;
    }

    public boolean rmDirMon() {
        boolean bRet = false;
        log.debug("Rm dir mon start ===>");
        bRet = operMon();
        log.debug("Rm dir mon end ===>");
        return bRet;
    }

    public boolean createFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Create file cmd start ===>");
        bRet = setClientOperType(OPER_CREATE_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Create file cmd end ===>");
        return bRet;
    }

    public boolean createFileCmdStop() {
        boolean bRet = false;
        log.debug("Create file cmd stop start ===>");
        bRet = Proc.proStartBase(CLIENT_IP, STOP_OPER_CMD, CLIENT_HOME);
        log.debug("Create file cmd stop end ===>");
        return bRet;
    }

    public boolean saveSmallFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Save small file cmd start ===>");
        bRet = setClientOperType(OPER_SAVE_SMALL_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Save file cmd end ===>");
        return bRet;
    }

    public boolean saveSmallFileCmdStop() {
        boolean bRet = false;
        log.debug("Save small file cmd stop start ===>");
        bRet = Proc.proStartBase(CLIENT_IP, STOP_OPER_CMD, CLIENT_HOME);
        log.debug("Save small file cmd stop end ===>");
        return bRet;
    }

    public boolean saveSmallFileMon() {
        boolean bRet = false;
        log.debug("Save small file mon start ===>");
        bRet = operMon();
        log.debug("Save small file mon end ===>");
        return bRet;
    }

    public boolean saveLargeFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Save large file cmd start ===>");
        bRet = setClientOperType(OPER_SAVE_LARGE_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Save file cmd end ===>");
        return bRet;
    }

    public boolean saveLargeFileCmdStop() {
        boolean bRet = false;
        log.debug("Save large file cmd stop start ===>");
        bRet = Proc.proStartBase(CLIENT_IP, STOP_OPER_CMD, CLIENT_HOME);
        log.debug("Save file cmd stop end ===>");
        return bRet;
    }

    public boolean lsFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Ls file cmd start ===>");
        bRet = setClientOperType(OPER_LS_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Ls file cmd end ===>");
        return bRet;
    }

    public boolean lsFileMon() {
        boolean bRet = false;
        log.debug("Ls file mon start ===>");
        bRet = operMon();
        log.debug("Ls file mon end ===>");
        return bRet;
    }
    public boolean fetchFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Fetch file cmd start ===>");
        bRet = setClientOperType(OPER_FETCH_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Fetch file cmd end ===>");
        return bRet;
    }

    public boolean fetchFileMon() {
        boolean bRet = false;
        log.debug("Fetch file mon start ===>");
        bRet = operMon();
        log.debug("Fetch file mon end ===>");
        return bRet;
    }

    public boolean mvFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Mv file cmd start ===>");
        bRet = setClientOperType(OPER_MV_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Mv file cmd end ===>");
        return bRet;
    }

    public boolean mvFileMon() {
        boolean bRet = false;
        log.debug("Mv file mon start ===>");
        bRet = operMon();
        log.debug("Mv file mon end ===>");
        return bRet;
    }

    public boolean rmFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Rm file cmd start ===>");
        bRet = setClientOperType(OPER_RM_FILE);
        if (false == bRet) return false;
        bRet = setClientUserId(userId);
        if (false == bRet) return false;
        String cmd = START_CLIENT_CMD + " >log." + caseName + "." + userId;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Rm file cmd end ===>");
        return bRet;
    }

    public boolean rmFileMon() {
        boolean bRet = false;
        log.debug("Rm file mon start ===>");
        bRet = operMon();
        log.debug("Rm file mon end ===>");
        return bRet;
    }

    public boolean operMon() {
        boolean bRet = false;
        for(;;) {
          int iRet = Proc.proMonitorBase(CLIENT_IP, START_CLIENT_CMD);
          if (0 == iRet) {
            bRet = true;
            break;
          }
          else if (iRet < 0) {
            bRet = false;
            break;
          }
        }
        return bRet;
    }

    public boolean mixOpCmd() {
        boolean bRet = false;
        log.debug("Mix operation cmd start ===>");
        String cmd = "./meta_oper.sh start_oper &>log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Mix operation cmd end ===>");
        return bRet;
    }

    public boolean mixOpCmdStop() {
        boolean bRet = false;
        log.debug("Mix operation cmd stop start ===>");
        bRet = Proc.proStartBase(CLIENT_IP, STOP_OPER_CMD, CLIENT_HOME);
        log.debug("Mix operation cmd stop end ===>");
        return bRet;
    }

   public boolean genPerfReport(String operType, long userId, long processCount) {
       boolean bRet = false;
       log.debug("Gen performance report cmd start ===>");
       String cmd = "/bin/bash " + PERF_SCRIPT + " log." + caseName + " " + userId + " " +  processCount + " >log." + caseName + "." + operType + ".perf_report";
       bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
       log.debug("Gen performance report cmd end ===>");
       return bRet;
   }

    // query db, check entry exist
    public boolean queryDB(String listName, long userId) {
        boolean bRet = false;
        // execute script
        String cmd = "cd " + CLIENT_HOME + "; ./" + MYSQL_SCRIPT + " query " + listName + userId;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(CLIENT_IP, cmd, result);
        if (false == bRet || result.size() > 0)
          return false;
        return bRet;
    }

    // clean entry from db
    public boolean clearDB(String listName, long userId) {
        boolean bRet = false;
        // execute script
        String cmd = "./" + MYSQL_SCRIPT + " clear " + listName + userId;
        bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
        if (false == bRet)
          return false;
        return bRet;
    }

    // clean all entry of one appId from db
    public boolean clearDB(long appId) {
        boolean bRet = false;
        // execute script
        String cmd = "./" + MYSQL_SCRIPT + " clear_app_id " + appId;
        bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
        if (false == bRet)
          return false;
        return bRet;
    }

    // move fileList name
    public boolean mvFileList(String fileName, long userId) {
      boolean bRet = false;
      String filePath = CLIENT_HOME + "/fileName";
      bRet = File.fileCopy(CLIENT_IP, filePath, filePath + "." + userId);
      if (bRet == false) return bRet;
      bRet = File.fileDel(CLIENT_IP, filePath);
      return bRet;
    }

    // get cache size
    public int getCacheSize(String metaServerIp) {
        int cacheSize;
        boolean bRet = false;
        String keyWord = KW_CACHE_SIZE;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.scanTailBase(metaServerIp, MS_LOG, KW_CACHE_SIZE, TAIL_LINE, COL_CACHE_SIZE, null, result);
        if (false == bRet || result.size() < 1)
          return 0;
        return Integer.parseInt(result.get(result.size() - 1));
    }

    public int getUsedSize(String metaServerIp) {
        boolean bRet = false;
        String keyWord = KW_CACHE_SIZE;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.scanTailBase(metaServerIp, MS_LOG, KW_CACHE_SIZE, TAIL_LINE, COL_USED_SIZE, null, result);
        if (false == bRet || result.size() < 1)
          return 0;
        return Integer.parseInt(result.get(result.size() - 1));
    }

    public boolean chkCacheGc(String metaServerIp, long appId, long userId, int gcPoint) {
        boolean bRet = false;
        int cacheUsedIncrement = gcPoint;
        int usedSizeLast = 0;
        int usedSize = getUsedSize(metaServerIp); // unit Byte
        if (usedSize <= 0) return false;
        log.debug("used cache size: " + usedSize + ", cache gc point: " + gcPoint);
        usedSizeLast = usedSize;

        while (usedSize < gcPoint) {
          sleep(250);
          usedSize = getUsedSize(metaServerIp);
          if (usedSize <= 0) return false;
          log.debug("used cache size: " + usedSize + ", cache gc point: " + gcPoint);

          int tmpIncrement = usedSize - usedSizeLast; 
          if (tmpIncrement < cacheUsedIncrement) {
            cacheUsedIncrement = tmpIncrement;
          }
          usedSizeLast = usedSize;

          // stop client
          bRet = createDirCmdStop();
          Assert.assertTrue(bRet);

          bRet = chkGcHappened(metaServerIp, appId, userId);
          if (bRet) return false;

          // will soon gc, changed to another userId
          if ((gcPoint - usedSize) < cacheUsedIncrement) {
            log.debug("@@ Soon reach cache gc point! used cache size: " + usedSize + ", cache gc point: " + gcPoint);
            return true;
          }

          // restart client
          bRet = createDirCmd(userId);
          if (false == bRet) return bRet;
        }
        return true;
    }

    public boolean waitUntilCacheGc(String metaServerIp, int gcPoint) {
        int cacheUsedIncrement = gcPoint;
        int usedSizeLast = 0;
        int usedSize = getUsedSize(metaServerIp); // unit Byte
        if (usedSize <= 0) return false;
        log.debug("used cache size: " + usedSize + ", cache gc point: " + gcPoint);
        usedSizeLast = usedSize;

        while (usedSize < gcPoint) {
          sleep(250);
          usedSize = getUsedSize(metaServerIp);
          if (usedSize <= 0) return false;

          int tmpIncrement = usedSize - usedSizeLast; 
          if (tmpIncrement < cacheUsedIncrement) {
            cacheUsedIncrement = tmpIncrement;
          }
          usedSizeLast = usedSize;

          if ((gcPoint - usedSize) < cacheUsedIncrement) {
            break;
          }
          log.debug("used cache size: " + usedSize + ", cache gc point: " + gcPoint);
        }
        log.debug("@@ Soon reach cache gc point! used cache size: " + usedSize + ", cache gc point: " + gcPoint);
        return true;
    }

    public boolean chkGcHappened(String metaServerIp, long appId, long userId) {
        boolean bRet = false;

        sleep(10);

        // check
        String cmd = "grep \\\"gc app_id " + appId + " uid " + userId + " root\\\" " + MS_LOG + "| wc -l";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int gcCount = Integer.parseInt(result.get(0));
        if (0 == gcCount)
          return false;
        return true;
    }

    // check gc hannpend and gc the right number of users
    public boolean chkGcHappened(String metaServerIp, long appId, int gcCount) {
        boolean bRet = false;

        sleep(10);

        // check
        String cmd = "grep \\\"gc app_id " + appId + " \\\" " + MS_LOG + "| wc -l";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int actualCount = Integer.parseInt(result.get(0));
        log.debug("actual gc count: " + actualCount + ", should be: " + gcCount);
        if (actualCount != gcCount)
          return false;
        return true;
    } 

    // check gc hannpend and happening right in sequence
    public boolean chkGcHappened(String metaServerIp, long appId, long [] gcUsers) {
        boolean bRet = false;

        sleep(10);

        // check
        ArrayList<String> keywords = new ArrayList<String>();
        keywords.add(KW_CACHE_GC + appId);
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.scanAllBase(metaServerIp, MS_LOG, keywords, COL_GC_USER, null, result);
        if (false == bRet || result.size() != gcUsers.length)
          return false;
        for (int i = 0; i < gcUsers.length; i++) {
          if (Long.parseLong(result.get(i)) != gcUsers[i]) {
             return false;
          }
        }
        return true;
    } 

    // get mutex result
    public boolean getMutexResult(String metaServerIp, int maxMutexSize) {
        boolean bRet = false;
        String cmd = "grep \\\"get mutex\\\" " + MS_LOG + " \"|awk '{print $NF}'|sort|uniq|wc -l\"";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int actualMutexSize = Integer.parseInt(result.get(0));
        log.debug("actual mutex size: " + actualMutexSize + ", max mutex size: " + maxMutexSize);
        if (actualMutexSize < 2 || actualMutexSize > maxMutexSize)
          return false;
        return true;
    }

    public boolean chkFreeListCap(String metaServerIp, int freeListCount) {
        boolean bRet = false;
        String cmd = "grep \\\"put MemNodeList success\\\" " + MS_LOG + "\"|wc -l\"";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int iResult = Integer.parseInt(result.get(0));
        if (iResult < 1)
          return false;

        result.clear();
        cmd = "grep \\\"put MemNodeList fail, size: " + freeListCount + ", capacity: " + freeListCount + "\\\" " + MS_LOG + "\"|wc -l\"";
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        iResult = Integer.parseInt(result.get(0));
        if (iResult < 1)
          return false;

        return true;
    }

    public boolean chkDbPoolSize(String metaServerIp, int maxSpoolSize) {
        boolean bRet = false;
        String cmd = "grep \\\"database_helper\\\" " + MS_LOG + " \"|wc -l\"";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int actualDbPoolSize = Integer.parseInt(result.get(0));
        if (maxSpoolSize > LIMIT_MAX_SPOOL_SIZE) {
           maxSpoolSize = LIMIT_MAX_SPOOL_SIZE;
        }
        if (actualDbPoolSize != maxSpoolSize)
          return false;
        return true;
    }

   public boolean chkRateEnd(float std, int operType, long userId) {
        float result = 0;
        if ((operType & OPER_CREATE_DIR) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName + "." + userId, KW_CREATE_DIR_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("create dir success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("create dir success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_LS_DIR) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName + "." + userId, KW_LS_DIR_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("ls dir success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("ls dir success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_LS_FILE) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName + "." + userId, KW_LS_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("ls file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("ls file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_SAVE_SMALL_FILE) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName + "." + userId, KW_SAVE_SMALL_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("save small file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("save small file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }


        if ((operType & OPER_FETCH_FILE) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName + "." + userId, KW_FETCH_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("fetch file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("fetch file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

 
        return false;
    }

    public float getRateEnd(String tarIp, String fileName, String keyWord) {
      boolean bRet = false;
      float fRet = -1;
      ArrayList<String> filter = new ArrayList<String>();
      ArrayList<Float> result = new ArrayList<Float>();
      filter.add("%");
      filter.add(",");
      bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAIL_LINE, COL_TAIL_RATE, filter, result);
      if ((bRet == false) || (result.size() < 1))
      {
        return fRet;
      }
      fRet = result.get(result.size() - 1);
      return fRet;  
    }

    public float getRateRun(String tarIp, String fileName, String keyWord) {
      boolean bRet = false;
      float fRet = 0;
      ArrayList<String> filter = new ArrayList<String>();
      ArrayList<Float> result = new ArrayList<Float>();
      filter.add("%");
      filter.add(",");
      bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAIL_LINE, COL_FAIL_COUNT, filter, result);
      if ((bRet == false) || (result.size() < 2))
      {
        return fRet;
      }
      int failCount = result.get(result.size() - 1) - result.get(result.size() - 2);
      if (failCount == 0) {
        fRet = 100;
      }
      return fRet;  
    }

    public boolean chkRateRunByLog(float std, int operType, String logName) {
        float result = 0;
        if ((operType & OPER_CREATE_DIR) != 0) {
            result = getRateRun(CLIENT_IP, logName, KW_CREATE_DIR_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("create dir success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("create dir success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_LS_DIR) != 0) {
            result = getRateRun(CLIENT_IP, logName, KW_LS_DIR_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("ls dir success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("ls dir success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_LS_FILE) != 0) {
            result = getRateRun(CLIENT_IP, logName, KW_LS_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("ls file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("ls file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

        if ((operType & OPER_SAVE_SMALL_FILE) != 0) {
            result = getRateRun(CLIENT_IP, logName, KW_SAVE_SMALL_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("save small file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("save small file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }


        if ((operType & OPER_FETCH_FILE) != 0) {
            result = getRateRun(CLIENT_IP, logName, KW_FETCH_FILE_STATIS);
            if (result == -1) {
                return false;
            }
            if (result != std) {
                log.error("fetch file success rate(" + result + "%) is not " + std + "% !!!");
                return false;
            }
            else {
                log.info("fetch file success rate(" + result + "%) is " + std + "% !!!");
            }
            return true;
        }

 
        return false;
    }

    public boolean chkRateRun(float std, int operType, long userId) {
        boolean bRet = false;
        String sorLog = CLIENT_LOG + caseName + "." + userId;
        String tmpLog = CLIENT_LOG + caseName + ".tmp." + userId;
        String cmd = "tail -f " + sorLog;
        bRet = Proc.proStartBack(CLIENT_IP, cmd + " > " + tarLog);
        if (bRet == false) return bRet;

        /* Wait */
        sleep(SCANTIME);
        
        bRet = Proc.proStopByCmd(CLIENTIP, cmd);
        if (bRet == false) return bRet;

        /* check the result */
        bRet = chkRateRunByLog(std, operType, tmpLog);
        return bRet;
    }

    public long getClientCurrentTime()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_cur_time " + logName;
        bRet = Proc.cmdOutBase(CLIENT_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));			
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getFailStartTime(long fromRowNum)
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_fail_start " + logName + " " + fromRowNum;
        bRet = Proc.cmdOutBase(CLIENT_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getFailEndTime(long fromRowNum)
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_fail_end " + logName + " " + fromRowNum;
        bRet = Proc.cmdOutBase(CLIENT_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));     
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getRsCurrentVersion(int index)
    {
        boolean bRet = false;
        long iRet = 0;
        ArrayList<String> result = new ArrayList<String>();
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(index).getIp();
        String rsLog = nameMetaGrid.getCluster(RSINDEX).getServer(index).getLogs();
        int type = 0;
        bRet = HA.chkVipBase(rsIp, VIP_ETH_NAME);
        if (bRet) { // vip on master
          type = 0;
        }
        else {
          type = 1;
        }
        String cmd = "/home/admin/workspace/chuyu/meta_oper.sh get_cur_version " + rsIp + " " + rsLog + " " + type;
        bRet = Proc.cmdOutBase(rsIp, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getRsCurrentVersion()
    {
        boolean bRet = false;
        long iRet = 0;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = "/home/admin/workspace/chuyu/meta_oper.sh get_cur_version " + MASTER_RS_IP + " " + RS_LOG;
        bRet = Proc.cmdOutBase(MASTER_RS_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));			
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public boolean clearOneRs(int index)
    {
        boolean bRet = false;
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(index).getIp();
        String logName = CLIENT_HOME + "/rootserver/table";
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh clear_rs " + logName;
        bRet = Proc.cmdOutBase(rsIp, cmd, null, 1, null, result);
        return bRet;
    }

    public long getClientCurrentRowNum()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_cur_row " + logName;
        bRet = Proc.cmdOutBase(CLIENT_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));			
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getRsCurrentVersion(int index)
    {
        boolean bRet = false;
        long iRet = 0;
        ArrayList<String> result = new ArrayList<String>();
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(index).getIp();
        String rsLog = nameMetaGrid.getCluster(RSINDEX).getServer(index).getLogs();
        int type = 0;
        bRet = HA.chkVipBase(rsIp, VIP_ETH_NAME);
        if (bRet) { // vip on master
          type = 0;
        }
        else {
          type = 1;
        }
        String cmd = "/home/admin/workspace/chuyu/meta_oper.sh get_cur_version " + rsIp + " " + rsLog + " " + type;
        bRet = Proc.cmdOutBase(rsIp, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public long getRsCurrentVersion()
    {
        boolean bRet = false;
        long iRet = 0;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = "/home/admin/workspace/chuyu/meta_oper.sh get_cur_version " + MASTER_RS_IP + " " + RS_LOG;
        bRet = Proc.cmdOutBase(MASTER_RS_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));			
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    public boolean clearOneRs(int index)
    {
        boolean bRet = false;
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(index).getIp();
        String logName = CLIENT_HOME + "/rootserver/table";
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh clear_rs " + logName;
        bRet = Proc.cmdOutBase(rsIp, cmd, null, 1, null, result);
        return bRet;
    }

    public long getClientCurrentRowNum()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_cur_row " + logName;
        bRet = Proc.cmdOutBase(CLIENT_IP, cmd, null, 1, null, result);
        if (bRet == false) return -1;
        try{
          iRet = Long.valueOf(result.get(result.size() - 1));			
          if (iRet > 0)
          {
            bRet = true;
            return iRet;
          }
        } catch (Exception e){
          e.printStackTrace();
        }
        return iRet;
    }

    // start & kill meta server related
    public boolean restartOneMetaServer(int index) {
      boolean bRet = false;
      log.info("Restart one meta start ===>");
      bRet = killOneServer(nameMetaGrid, MSINDEX, index);
      if (bRet == false) return bRet;
      bRet = startOneServer(nameMetaGrid, MSINDEX, index);
      log.info("Restart one meta end ===>");
      return bRet;
    }
    // start & kill meta server related                                                                                                                                 
    public boolean killOneMetaserver(int index)
    {
      boolean bRet = false;
      log.info("Kill one meta start ===>");
      if (nameMetaGrid == null)
      {
        log.debug("nameMetaGrid is null");
      }
      bRet = killOneServer(nameMetaGrid, MSINDEX, index);
      log.info("Kill one meta end ===>");
      return bRet;
    }

    public boolean startOneMetaserver(int index)
    {
      boolean bRet = false;
      log.info("start one meta start ===>");
      if (nameMetaGrid == null)
      {
        log.debug("nameMetaGrid is null");
      }
      bRet = startOneServer(nameMetaGrid, MSINDEX, index);
      log.info("start one meta end ===>");
      return bRet;
    }

    public boolean cleanOneMetaServer(int index) {
      boolean bRet = false;
      log.info("Clean one meta start ===>");
      bRet = cleanOneServer(nameMetaGrid, MSINDEX, index);
      log.info("Clean one meta end ===>");
      return bRet;
    }

    public boolean cleanOneMetaServerForce(int index) {
      boolean bRet = false;
      log.info("Clean one meta force start ===>");
      bRet = cleanOneServerForce(nameMetaGrid, MSINDEX, index);
      log.info("Clean one meta force end ===>");
      return bRet;
    }

    public boolean restartMetaServerCluster() {
      boolean bRet = false;
      log.info("restart meta server cluster start ===>");
      bRet = restartOneCluster(nameMetaGrid, MSINDEX);
      log.info("restart meta server cluster end ===>");
      return bRet;
    }

    // block network related
    public boolean blockClientrToMS(int msIndex) {
        String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(msIndex).getIp(); 
        boolean bRet = Proc.portOutputBlock(CLIENT_IP, msIp, MSPORT); 
        return bRet;
    }

    public boolean unblockClientToMS() {
        boolean bRet = Proc.netUnblockBase(CLIENT_IP); 
        return bRet;
    }

    public boolean blockMetaServerToRS(int msIndex) {
        String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(msIndex).getIp(); 
        boolean bRet = Proc.portOutputBlock(msIp, MASTER_RS_IP, RSPORT); 
        return bRet;
    } 

    public boolean unblockMetaServerToRS(int msIndex) {
        String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(msIndex).getIp(); 
        boolean bRet = Proc.netUnblockBase(msIp); 
        return bRet;
    }

    public boolean fullBlockMetaServerAndRS(int msIndex) {
        String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(msIndex).getIp(); 
        boolean bRet = Proc.netFullBlockBase(msIp, RSVIP); 
        return bRet;
    }

    public boolean fullBlockClientrToMS(int msIndex) {
        String msIp = nameMetaGrid.getCluster(MSINDEX).getServer(msIndex).getIp(); 
        boolean bRet = Proc.netFullBlockBase(CLIENT_IP, msIp); 
        return bRet;
    }

    // HA related

    public int getHaMasterRsIndex() {
      int index = -1;
      for (int i = 0; i < RS_MAX_COUNT; i++) {
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(i).getIp();
        boolean bRet = HA.chkVipBase(rsIp, VIP_ETH_NAME);
        if (bRet) { // vip on master
          index = i;
        }
      }
      return index;
    }

    public int getHaSlaveRsIndex() {
      int index = -1;
      for (int i = 0; i < RS_MAX_COUNT; i++) {
        String rsIp = nameMetaGrid.getCluster(RSINDEX).getServer(i).getIp();
        boolean bRet = HA.chkVipBase(rsIp, VIP_ETH_NAME);
        if (!bRet) { // vip on master
          index = i;
        }
      }
      return index;
    }

    public boolean killOneRs(int index) {
        boolean bRet = false;
        log.info("Kill one rs start ===>");
        if (nameMetaGrid == null)
        {
          log.debug("nameMetaGrid is null");
        }
        bRet = killOneServer(nameMetaGrid, RSINDEX, index);
        log.info("Kill one rs end ===>");
        return bRet;
    }

    public boolean startOneRs(int index) {
        boolean bRet = false;
        log.info("start one rs start ===>");
        if (nameMetaGrid == null)
        {
          log.debug("nameMetaGrid is null");
        }
        bRet = startOneServer(nameMetaGrid, RSINDEX, index);
        log.info("start one rs end ===>");
        return bRet;
    }

    public boolean migrateHaVip(int masterRsIndex, int slaveRsIndex) {
        boolean bRet = false;
        String rsVip = nameMetaGrid.getCluster(RSINDEX).getServer(masterRsIndex).getVip();
        String masterRsIp = nameMetaGrid.getCluster(RSINDEX).getServer(masterRsIndex).getIp();
        String slaveRsIp = nameMetaGrid.getCluster(RSINDEX).getServer(slaveRsIndex).getIp();
        log.debug("masterRsIp: " + masterRsIp + ", slaveRsIp: " + slaveRsIp);
        String cmd = "";
        cmd = "/sbin/ifconfig bond0:1 down";
        bRet = Proc.proStartBaseByRoot(slaveRsIp, cmd);
        if (!bRet) {
          log.debug("stop master process failed");
        }
        else {
          cmd = "/sbin/ifconfig bond0:1 " + rsVip + " netmask 255.255.255.255";
          bRet = Proc.proStartBaseByRoot(masterRsIp, cmd);
          if (!bRet) {
            log.debug("stop slave process failed");
          }
        }
        return bRet; 
    }

    public boolean resetRsFailCount(int index) {
        AppServer rs = nameMetaGrid.getCluster(RSINDEX).getServer(index);
        return resetFailCount(rs);
    }


    public boolean killMasterRs() {
        boolean bRet = false;
        AppServer tmpMaster = null;
        AppServer tmpSlave = null;

        /* Find the master rootserver */
        bRet = HA.chkVipBase(MASTER_RS_IP, VIP_ETH_NAME);
        if (bRet) { // vip on master
            bRet = MASTER_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            tmpMaster = SLAVE_RS;
            tmpSlave = MASTER_RS;
        }
        else { // vip on slave
            bRet = SLAVE_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            tmpMaster = MASTER_RS;
            tmpSlave = SLAVE_RS;
        }
        
        /* Wait for vip migrate */
        sleep (MIGRATE_TIME);
        
        /* Check vip */
        bRet = HA.chkVipBase(tmpSlave.getIp(), VIP_ETH_NAME);
        if (bRet == true) {
            log.error("VIP is not migrate yet!!!");
            return false;
        }
        
        /* Reset the failcount */
        bRet = resetFailCount(tmpSlave);
        if (bRet == false) return bRet;
        
        /* Set the new vip */
        MASTER_RS = tmpMaster;
        SLAVE_RS = tmpSlave;
        
        MASTER_RS_IP = MASTER_RS.getIp();
        SLAVE_RS_IP = SLAVE_RS.getIp();
 
        return bRet;
    }

    public boolean cleanMasterRs() {
        boolean bRet = false;
        AppServer tmpMaster = null;
        AppServer tmpSlave = null;

        /* Find the master rootserver */
        bRet = HA.chkVipBase(MASTER_RS_IP, VIP_ETH_NAME);
        if (bRet) { // vip on master
            bRet = MASTER_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            bRet = MASTER_RS.clean();
            if (bRet == false) return bRet;
            tmpMaster = SLAVE_RS;
            tmpSlave = MASTER_RS;
        }
        else { // vip on slave
            bRet = SLAVE_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            bRet = SLAVE_RS.clean();
            if (bRet == false) return bRet;
            tmpMaster = MASTER_RS;
            tmpSlave = SLAVE_RS;
        }
        
        /* Wait for vip migrate */
        sleep (MIGRATE_TIME);
        
        /* Check vip */
        bRet = HA.chkVipBase(tmpSlave.getIp(), VIP_ETH_NAME);
        if (bRet == true) {
            log.error("VIP is not migrate yet!!!");
            return false;
        }
        
        /* Reset the failcount */
        bRet = resetFailCount(tmpSlave);
        if (bRet == false) return bRet;
        
        /* Set the new vip */
        MASTER_RS = tmpMaster;
        SLAVE_RS = tmpSlave;
        
        MASTER_RS_IP = MASTER_RS.getIp();
        SLAVE_RS_IP = SLAVE_RS.getIp();
 
        return bRet;
    }
    
    public boolean killSlaveRs() {
        boolean bRet = false;
        AppServer tmpMaster = null;
        AppServer tmpSlave = null;

        /* Find the master rootserver */
        bRet = HA.chkVipBase(MASTER_RS_IP, VIP_ETH_NAME);
        if (bRet) { // vip on master
            bRet = SLAVE_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            tmpMaster = MASTER_RS;
            tmpSlave = SLAVE_RS;
        }
        else { // vip on slave
            bRet = MASTER_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            tmpMaster = SLAVE_RS;
            tmpSlave = MASTER_RS;
        }
        
        /* Reset the failcount */
        bRet = resetFailCount(tmpSlave);
        if (bRet == false) return bRet;

        /* Set the new vip */
        MASTER_RS = tmpMaster;
        SLAVE_RS = tmpSlave;

        MASTER_RS_IP = MASTER_RS.getIp();
        SLAVE_RS_IP = SLAVE_RS.getIp();
 
        return bRet;
    }

    public boolean cleanSlaveRs() {
        boolean bRet = false;
        AppServer tmpMaster = null;
        AppServer tmpSlave = null;

        /* Find the master rootserver */
        bRet = HA.chkVipBase(MASTER_RS_IP, VIP_ETH_NAME);
        if (bRet) { // vip on master
            bRet = SLAVE_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            bRet = SLAVE_RS.clean();
            if (bRet == false) return bRet;
            tmpMaster = MASTER_RS;
            tmpSlave = SLAVE_RS;
        }
        else { // vip on slave
            bRet = MASTER_RS.stop(KillTypeEnum.NORMALKILL, WAIT_TIME);
            if (bRet == false) return bRet;
            bRet = MASTER_RS.clean();
            if (bRet == false) return bRet;
            tmpMaster = SLAVE_RS;
            tmpSlave = MASTER_RS;
        }

        /* Reset the failcount */
        bRet = resetFailCount(tmpSlave);
        if (bRet == false) return bRet;

        /* Set the new vip */
        MASTER_RS = tmpMaster;
        SLAVE_RS = tmpSlave;

        MASTER_RS_IP = MASTER_RS.getIp();
        SLAVE_RS_IP = SLAVE_RS.getIp();
 
        return bRet;
    }
    
    public boolean startSlaveRs() {
        boolean bRet = false;
        bRet = SLAVE_RS.start();
        return bRet;
    }
}
