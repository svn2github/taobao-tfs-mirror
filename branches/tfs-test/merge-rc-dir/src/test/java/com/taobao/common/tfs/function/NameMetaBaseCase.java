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
    final public int RSINDEX = 0;
    final public int MSINDEX = 1;
    final public int META_COUNT = 3;
    final public String RSVIP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getVip();
    final public String MASTER_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getIp();
    //final public String SLAVE_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(1).getIp();
    final public String SLAVE_RS_IP = "";
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

    // operation type
    public int OPER_CREATE_DIR = 1;
    public int OPER_LS_DIR = 2;
    public int OPER_SAVE_SMALL_FILE = 4;
    public int OPER_SAVE_LARGE_FILE = 8;
    public int OPER_FETCH_FILE = 16;
    public int OPER_LS_FILE = 32;
    public int OPER_UNLINK = 64;
    public int OPER_MIX = 128;
 
    final public String CLIENT_IP = nameMetaClient.getIp();
    final public String CLIENT_HOME = nameMetaClient.getDir();
    final public String CLIENT_LOG = nameMetaClient.getLogs();
    final public String MYSQL_SCRIPT = "do_mysql.sh";
    final public String CREATE_DIR_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_CREATE_DIR;
    final public String LS_DIR_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_LS_DIR;
    final public String SAVE_SMALL_FILE_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_SAVE_SMALL_FILE;
    final public String SAVE_LARGE_FILE_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_SAVE_LARGE_FILE;
    final public String FETCH_FILE_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_FETCH_FILE;
    final public String LS_FILE_CMD = "/bin/bash meta_oper.sh start_oper " + OPER_LS_FILE;
    final public String STOP_OPER_CMD = "/bin/bash meta_oper.sh stop_oper";

    final public String SAVED_FILE_LIST = "oper_save_file.fileList.";
    final public String CREATED_DIR_LIST = "oper_create_dir.fileList.";

    // time
    final public int LEASE_TIME = 6;

    // keywords
    final public String KW_SERVING_MS_IP = "to metaServer";
    final public String KW_APP_ID = "appId: ";
    final public String KW_USER_ID = "userId: ";
    final public String KW_LS_DIR_STATIS = "oper_ls_dir stat info";
    final public String KW_LS_FILE_STATIS = "oper_ls_file stat info";
    final public String KW_FETCH_FILE_STATIS = "oper_fetch_file stat info";
    final public String KW_CREATE_DIR_STATIS = "oper_create_dir stat info";
    final public String KW_CACHE_SIZE = "malloc size";
    final public String KW_CACHE_GC = "gc app_id ";

    // columns
    final public int MS_IP_COL = 12; //TODO: 
    final public int TAIL_RATE_COL = 15;
    final public int CACHE_SIZE_COL = 13;
    final public int USED_SIZE_COL = 17;
    final public int GC_USER_COL = 10;

    // metaServer config items
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

    // other
    final public int TAIL_LINE = 1000;

    @BeforeClass
    public  static void setUpOnce() throws Exception {
      boolean bRet = false;
      /* Kill the grid */
      //bRet = nameMetaGrid.stop(KillTypeEnum.FORCEKILL, WAIT_TIME);
      //Assert.assertTrue(bRet);

      ///* Clean the log file */
      //bRet = nameMetaGrid.clean();
      //Assert.assertTrue(bRet);

      //bRet = nameMetaGrid.start();
      //Assert.assertTrue(bRet);
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
    }

    // fuctions

    // set metaServer conf
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
        String cmd = "head -1000 " + logName;
        bRet = Proc.cmdOutBase2(CLIENT_IP, cmd, keyWords, MS_IP_COL, filter, result); 
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
        String cmd = CREATE_DIR_CMD + " " + userId + " >log." + caseName;
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

    public boolean saveSmallFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Save small file cmd start ===>");
        String cmd = SAVE_SMALL_FILE_CMD + " " + userId + " >log." + caseName;
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

    public boolean saveLargeFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Save large file cmd start ===>");
        String cmd = SAVE_LARGE_FILE_CMD + " " + userId + " >log." + caseName;
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

    public boolean fetchFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Fetch file cmd start ===>");
        String cmd = FETCH_FILE_CMD + " " + userId + " >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Fetch file cmd end ===>");
        return bRet;
    }

    public boolean fetchFileMon() {
        boolean bRet = false;
        log.debug("Fetch file mon start ===>");
        for(;;) {
          int iRet = Proc.proMonitorBase(CLIENT_IP, FETCH_FILE_CMD);
          if (0 == iRet) {
            bRet = true;
            break;
          }
          else if (iRet < 0) {
            bRet = false;
            break;
          }
        }
        log.debug("Fetch file mon end ===>");
        return bRet;
    }


    public boolean lsDirCmd() {
        boolean bRet = false;
        log.debug("Ls dir cmd start ===>");
        String cmd = LS_DIR_CMD + " >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Ls dir cmd end ===>");
        return bRet;
    }

    public boolean lsDirMon() {
        boolean bRet = false;
        log.debug("Ls dir mon start ===>");
        for(;;) {
          int iRet = Proc.proMonitorBase(CLIENT_IP, LS_DIR_CMD);
          if (0 == iRet) {
            bRet = true;
            break;
          }
          else if (iRet < 0) {
            bRet = false;
            break;
          }
        }
        log.debug("Ls dir mon end ===>");
        return bRet;
    }

    public boolean lsFileCmd(long userId) {
        boolean bRet = false;
        log.debug("Ls file cmd start ===>");
        String cmd = LS_FILE_CMD + " " + userId + " >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Ls file cmd end ===>");
        return bRet;
    }

    public boolean lsFileMon() {
        boolean bRet = false;
        log.debug("Ls file mon start ===>");
        for(;;) {
          int iRet = Proc.proMonitorBase(CLIENT_IP, LS_FILE_CMD);
          if (0 == iRet) {
            bRet = true;
            break;
          }
          else if (iRet < 0) {
            bRet = false;
            break;
          }
        }
        log.debug("Ls file mon end ===>");
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

    // move log name
    public boolean mvLog(String operType) {
      boolean bRet = false;
      String filePath = CLIENT_LOG + caseName;
      bRet = File.fileCopy(CLIENT_IP, filePath, filePath + "." + operType);
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
        bRet = Proc.scanTailBase(metaServerIp, MS_LOG, KW_CACHE_SIZE, TAIL_LINE, CACHE_SIZE_COL, null, result);
        if (false == bRet || result.size() < 1)
          return 0;
        return Integer.parseInt(result.get(result.size() - 1));
    }

    public int getUsedSize(String metaServerIp) {
        boolean bRet = false;
        String keyWord = KW_CACHE_SIZE;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.scanTailBase(metaServerIp, MS_LOG, KW_CACHE_SIZE, TAIL_LINE, USED_SIZE_COL, null, result);
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
          sleep(360);
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
          sleep(360);
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
        String cmd = "grep \\\"gc app_id " + appId + " uid " + userId + " root\\\" " + MS_LOG + "*" + "| wc -l";
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
        String cmd = "grep \\\"gc app_id " + appId + " \\\" " + MS_LOG + "*" + "| wc -l";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(metaServerIp, cmd, result);
        if (false == bRet || 1 != result.size())
          return false;
        int actualCount = Integer.parseInt(result.get(0));
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
        bRet = Proc.scanAllBase(metaServerIp, MS_LOG + "*", keywords, GC_USER_COL, null, result);
        if (false == bRet || result.size() != gcUsers.length)
          return false;
        for (int i = 0; i < gcUsers.length; i++) {
          if (Long.parseLong(result.get(i)) != gcUsers[i]) {
             return false;
          }
        }
        return true;
    } 

    public boolean chkRateEnd(float std, int operType) {
        float result = 0;
        if ((operType & OPER_LS_DIR) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName, KW_LS_DIR_STATIS);
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
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName, KW_LS_FILE_STATIS);
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

        if ((operType & OPER_FETCH_FILE) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName, KW_FETCH_FILE_STATIS);
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

    public float getRateEnd(String tarIp, String fileName, String keyWord)
    {
      boolean bRet = false;
      float fRet = -1;
      ArrayList<String> filter = new ArrayList<String>();
      ArrayList<Float> result = new ArrayList<Float>();
      filter.add("%");
      filter.add(",");
      bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAIL_LINE, TAIL_RATE_COL, filter, result);
      if ((bRet == false) || (result.size() < 1))
      {
        return fRet;
      }
      fRet = result.get(result.size() - 1);
      return fRet;  
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

    public long getFailStartTime()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_fail_start " + logName;
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

    public long getFailEndTime()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = CLIENT_HOME + "/meta_oper.sh get_fail_end " + logName;
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
      if (bRet == false) return bRet;
      log.info("Clean one meta end ===>");
      return bRet;
    }

    // block network related
    public boolean blockClientrToMS(String metaServerIp) {
        boolean bRet = Proc.portOutputBlock(CLIENT_IP, metaServerIp, MSPORT); 
        return bRet;
    }

    public boolean unblockClientToMS() {
        boolean bRet = Proc.netUnblockBase(CLIENT_IP); 
        return bRet;
    }

    public boolean blockMetaServerToRS(String metaServerIp) {
        boolean bRet = Proc.portOutputBlock(metaServerIp, MASTER_RS_IP, RSPORT); 
        return bRet;
    } 

    public boolean unblockMetaServerToRS(String metaServerIp) {
        boolean bRet = Proc.netUnblockBase(metaServerIp); 
        return bRet;
    }

    public boolean fullBlockMetaServerAndRS(String metaServerIp) {
        boolean bRet = Proc.netFullBlockBase(metaServerIp, RSVIP); 
        return bRet;
    }

    // HA related
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

        return bRet;
    }
    
    public boolean startSlaveRs() {
        boolean bRet = false;
        bRet = SLAVE_RS.start();
        return bRet;
    }
}
