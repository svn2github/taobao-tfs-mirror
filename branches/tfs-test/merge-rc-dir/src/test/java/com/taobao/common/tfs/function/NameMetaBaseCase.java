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
    //final static AppGrid nameMetaGrid = (AppGrid)serverFactory.getBean("nameMetaGrid");

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
    final public String CLIENT_IP = "10.232.36.208";

    // Keywords
    final public String KW_SERVING_MS_IP = "to metaServer";
    final public String KW_APP_ID = "appId: ";
    final public String KW_USER_ID = "userId: ";

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
