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
 
    // Keywords
    final public String KW_SERVING_MS_IP = "to metaServer";
    final public String KW_APP_ID = "appId: ";
    final public String KW_USER_ID = "userId: ";
    final public String KW_LS_DIR_STATIS = "ls_dir statis: ";
    final public String KW_CREATE_DIR_STATIS = "create_dir statis: ";

    // columns
    final public int MS_IP_COL = 12; //TODO: 

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

    public boolean createDirCmd() {
        boolean bRet = false;
        log.debug("Create dir cmd start ===>");
        bRet = createDirClient.start();
        log.debug("Create dir cmd end ===>");
        return bRet;
    }

    public boolean createDirCmdStop() {
        boolean bRet = false;
        log.debug("Create dir cmd stop start ===>");
        bRet = createDirClient.stop();
        log.debug("Create dir cmd stop end ===>");
        return bRet;
    }

    public boolean lsDirCmd() {
        boolean bRet = false;
        log.debug("Ls dir cmd start ===>");
        bRet = lsDirClient.start();
        log.debug("Ls dir cmd end ===>");
        return bRet;
    }

    public boolean lsDirMon() {
        boolean bRet = false;
        log.debug("Ls dir mon start ===>");
        bRet = lsDirClient.stop();
        log.debug("Ls dir mon end ===>");
        return bRet;
    }

    public boolean mixOpCmd() {
        boolean bRet = false;
        log.debug("Mix operation cmd start ===>");
        bRet = mixOpClient.start();
        log.debug("Mix operation cmd end ===>");
        return bRet;
    }

    public boolean mixOpCmdStop() {
        boolean bRet = false;
        log.debug("Mix operation cmd stop start ===>");
        bRet = mixOpClient.stop();
        log.debug("Mix operation cmd stop end ===>");
        return bRet;
    }

    // query db, check entry exist
    public boolean verifyDb() {
        boolean bRet = false;
        // execute script
        String cmd = "";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(CLIENT_IP, cmd, result);
        return bRet;
    }

    // clean entry from db
    public boolean cleanDb() {
        boolean bRet = false;
        // execute script
        String cmd = "";
        ArrayList<String> result = new ArrayList<String>();
        bRet = Proc.proStartBase(CLIENT_IP, cmd, result);
        return bRet;
    }

    public boolean chkRateEnd(float std, int operType) {
        float result = 0;
        if (operType & OPER_LS_DIR) {
            result = getRateEnd(CLIENT_IP, CLIENT_LOG + caseName, KW_LS_DIR_STATIS);
        }
        if (result == -1) {
            return false;
        }
        if (result < std) {
            log.error("ls dir success rate(" + result + "%) is lower than " + std + "% !!!");
            return bRet;
        }
        else {
            log.info("ls dir success rate(" + result + "%) is higher than " + std + "% !!!");
        }

        return true;
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
