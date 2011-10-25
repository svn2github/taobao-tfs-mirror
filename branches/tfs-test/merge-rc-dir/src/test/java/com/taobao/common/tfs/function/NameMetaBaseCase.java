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
    final static AppServer createDirClient = (AppServer)clientFactory.getBean("createDirClient");
    final static AppServer lsDirClient = (AppServer)clientFactory.getBean("lsDirClient");
    final static AppServer mixOpClient = (AppServer)clientFactory.getBean("mixOpClient");

    // Define
    // server related
    final public int RSINDEX = 0;
    final public int MSINDEX = 1;
    final public int META_COUNT = 1;
    final public String RSVIP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getVip();
    final public String MASTER_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(0).getIp();
    //final public String SLAVE_RS_IP = nameMetaGrid.getCluster(RSINDEX).getServer(1).getIp();
    final public String SLAVE_RS_IP = "";
    final public int RSPORT = nameMetaGrid.getCluster(RSINDEX).getServer(0).getPort();
    final public int MSPORT = nameMetaGrid.getCluster(MSINDEX).getServer(0).getPort();
    final public String MSCONF = nameMetaGrid.getCluster(MSINDEX).getServer(0).getConfname();    
    public AppServer MASTER_RS = nameMetaGrid.getCluster(RSINDEX).getServer(0);
    //public AppServer SLAVE_RS = nameMetaGrid.getCluster(RSINDEX).getServer(1);
    public AppServer SLAVE_RS = null;

    // ha related
    final public String VIP_ETH_NAME = "eth0:1";

    // client related
    final public String CLIENT_IP = createDirClient.getIp();
    final public String CLIENT_HOME = createDirClient.getDir();
    final public String CLIENT_LOG = createDirClient.getLogs();

    // time
    final public int LEASE_TIME = 6;

    // operation type
    public int OPER_CREATE_DIR = 1;
    public int OPER_LS_DIR = 2;
    public int OPER_FETCH = 4;
    public int OPER_SAVE = 8;
    public int OPER_UNLINK = 16;
 
    // keywords
    final public String KW_SERVING_MS_IP = "to metaServer";
    final public String KW_APP_ID = "appId: ";
    final public String KW_USER_ID = "userId: ";
    final public String KW_LS_DIR_STATIS = "ls_dir statis: ";
    final public String KW_CREATE_DIR_STATIS = "create_dir statis: ";

    // columns
    final public int MS_IP_COL = 12; //TODO: 
    final public int TAIL_RATE_COL = 12; //TODO: 

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
    public String getServingMSIp(long appId, long userId) {
        boolean bRet = false;
        ArrayList<String> keyWords = new ArrayList<String>();
        keyWords.add(KW_SERVING_MS_IP); 
        keyWords.add(KW_APP_ID + appId); 
        keyWords.add(KW_USER_ID + userId); 
        String logName = ""; //TODO: client log
        ArrayList<String> filter = new ArrayList<String>();
        filter.add("/");
        ArrayList<String> result = new ArrayList<String>();
        String cmd = "cat " + logName;
        bRet = Proc.cmdOutBase2(CLIENT_IP, cmd, keyWords, MS_IP_COL, filter, result); 
        if (false == bRet || result.size() < 1) return null;
        return result.get(result.size() - 1);
    }

    public String getUnServingMSIp(long appId, long userId) {
        String retMsAddr = null;
        String targetMsAddr = getServingMSIp(appId, userId);
        for (int i = 0; i < META_COUNT; i++)
        {
          String msIp = nameMetaGrid.getCluster(RSINDEX).getServer(i).getIp(); 
          int msPort = nameMetaGrid.getCluster(RSINDEX).getServer(i).getPort(); 
          String msAddr = msIp + ":" + msPort;
          if (targetMsAddr != msAddr)
          {
            retMsAddr = msAddr;
            break; 
          }
        }
        return retMsAddr;
    }

    public long getServingMSIndex(long appId, long userId) {
        return getMsIndex(getServingMSIp(appId, userId));
    }

    public long getUnServingMSIndex(long appId, long userId) {
        return getMsIndex(getUnServingMSIp(appId, userId));
    }

    private long getMsIndex(String targetMsAddr)
    {
        int index = -1;
        for (int i = 0; i < META_COUNT; i++)
        {
          String msIp = nameMetaGrid.getCluster(RSINDEX).getServer(i).getIp(); 
          int msPort = nameMetaGrid.getCluster(RSINDEX).getServer(i).getPort(); 
          String msAddr = msIp + ":" + msPort;
          if (targetMsAddr == msAddr)
          {
            index = i;
            break; 
          }
        }
        return index;
    }

    public boolean createDirCmd() {
        boolean bRet = false;
        log.debug("Create dir cmd start ===>");
        String cmd = "./meta_oper.sh start_oper >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Create dir cmd end ===>");
        return bRet;
    }

    public boolean createDirCmdStop() {
        boolean bRet = false;
        log.debug("Create dir cmd stop start ===>");
        String cmd = "./meta_oper.sh stop_oper";
        bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Create dir cmd stop end ===>");
        return bRet;
    }

    public boolean lsDirCmd() {
        boolean bRet = false;
        log.debug("Ls dir cmd start ===>");
        String cmd = "./meta_oper.sh start_oper >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Ls dir cmd end ===>");
        return bRet;
    }

    public boolean lsDirMon() {
        boolean bRet = false;
        log.debug("Ls dir mon start ===>");
        log.debug("Ls dir mon end ===>");
        return bRet;
    }

    public boolean mixOpCmd() {
        boolean bRet = false;
        log.debug("Mix operation cmd start ===>");
        String cmd = "./meta_oper.sh start_oper >log." + caseName;
        bRet = Proc.proStartBackroundBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Mix operation cmd end ===>");
        return bRet;
    }

    public boolean mixOpCmdStop() {
        boolean bRet = false;
        log.debug("Mix operation cmd stop start ===>");
        String cmd = "./meta_oper.sh stop_oper";
        bRet = Proc.proStartBase(CLIENT_IP, cmd, CLIENT_HOME);
        log.debug("Mix operation cmd stop end ===>");
        return bRet;
    }

    // query db, check entry exist
    public boolean queryDB(String fileListName) {
        boolean bRet = false;
        // execute script
        String cmd = "cd " + CLIENT_HOME + "; sh get_mysql_result.sh query_exist " + fileListName;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(CLIENT_IP, cmd, result);
        if (false == bRet || result.size() > 0)
          return false;
        return bRet;
    }

    // clean entry from db
    public boolean cleanDB(String fileListName) {
        boolean bRet = false;
        // execute script
        String cmd = "cd " + CLIENT_HOME + "; sh get_mysql_result.sh clean " + fileListName;
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(CLIENT_IP, cmd, result);
        if (false == bRet || result.size() > 0)
          return false;
        return bRet;
    }

    public boolean chkRateEnd(float std, int operType) {
        float result = 0;
        if ((operType & OPER_LS_DIR) != 0) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName, KW_LS_DIR_STATIS);
        }
        if (result == -1) {
            return false;
        }
        if (result < std) {
            log.error("ls dir success rate(" + result + "%) is lower than " + std + "% !!!");
            return false;
        }
        else {
            log.info("ls dir success rate(" + result + "%) is higher than " + std + "% !!!");
        }

        return true;
    }

    public float getRateEnd(String tarIp, String fileName, String keyWord)
    {
      boolean bRet = false;
      float fRet = -1;
      ArrayList<String> filter = new ArrayList<String>();
      ArrayList<Float> context = new ArrayList<Float>();
      filter.add("%");
      filter.add(",");
      filter.add("rate:");
      bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAIL_LINE, TAIL_RATE_COL, filter, context);
      if ((bRet == false) || (context.size() != 1))
      {
        return fRet;
      }
      fRet = context.get(0);
      return fRet;  
    }
 
    public long getClientCurrentTime()
    {
        boolean bRet = false;
        long iRet = 0;
        String logName = CLIENT_LOG + caseName;
        ArrayList<String> result = new ArrayList<String>();
        String cmd = "head -1 " + logName + " | awk -F '[[]' '{print $2}' | awk -F '[]]' '{print $1}' | date +%s";
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
        String cmd = "grep ERROR " + logName + " | head -1 | awk -F '[[]' '{print $2}' | awk -F '[]]' '{print $1}' | date +%s";
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
        String cmd = "grep ERROR " + logName + " | tail -1 | awk -F '[[]' '{print $2}' | awk -F '[]]' '{print $1}' | date +%s";
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
