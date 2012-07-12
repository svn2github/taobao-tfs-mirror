package com.taobao.common.tfs.nativetest;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.tfs.MultiClusterSyncBaseCase;
import com.taobao.gaia.KillTypeEnum;

/**
 * @author lexin
 */

public class Function_multi_cluster_sync_test extends MultiClusterSyncBaseCase {
	
	final public String clusterAIP = tfsGrid.getCluster(NSINDEX).getServer(0).getVip();
	final public String clusterBIP = tfsGrid2.getCluster(NSINDEX).getServer(0).getVip(); 
	//final public String clusterCIP = tfsGrid3.getCluster(NSINDEX).getServer(0).getVip(); 
	final public String clusterCIP = ""; 

	final public String clusterAAddr = clusterAIP + ":" + tfsGrid.getCluster(NSINDEX).getServer(0).getPort();
	final public String clusterBAddr = clusterBIP + ":" + tfsGrid2.getCluster(NSINDEX).getServer(0).getPort();
	//final public String clusterCAddr = clusterCIP + ":" + tfsGrid3.getCluster(NSINDEX).getServer(0).getPort();
	final public String clusterCAddr = "";
	
	
	
	public String writeOneTfsFile(String clusterVipAddr, String szFileName){
	      String strCmd = "/home/admin/tfs_bin/bin/tfstool -s " + clusterVipAddr + " -i 'put ";
	      strCmd += szFileName + "' | grep success";

	      ArrayList<String> szOutputList = new ArrayList<String>();
	      assertTrue(Proc.proStartBase(CLIENTIP, strCmd, szOutputList));
	      assertTrue(szOutputList.size() > 0);
	      
	      return szOutputList.get(0);
	   }

	   public String getOneTfsFile(String clusterVipAddr, String tfsFileName){
	      String strCmd = "/home/admin/tfs_bin/bin/tfstool -s " + clusterVipAddr + " -i 'get ";
	      strCmd += tfsFileName + " test_file.jpg" + "' | grep success";

	      ArrayList<String> szOutputList = new ArrayList<String>();
	      assertTrue(Proc.proStartBase(CLIENTIP, strCmd, szOutputList));
	      assertTrue(szOutputList.size() > 0);

	      return szOutputList.get(0);
	   }
	   /*test case for  */
	   @Test
	   public void test_multi_cluster_read(){
	      String szFileName = "test_sync.jpg";
	      createFile("test_sync.jpg", 10 * (1<<10));
	      
	      String tfsFileName = writeOneTfsFile(clusterAAddr, szFileName);
	      getOneTfsFile(clusterAAddr, tfsFileName);
	      
	      /* we get two file, one is test_sync.jpg, one is test_file.jpg*/
	      /* what we should do is to check the two file is the same */
	   }
/**/
	@Test
	public void Function_01_happy_sync() {

		boolean bRet = false;

		caseName = "Function_01_happy_sync";
		log.info(caseName + "===> start");

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write process */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait120 s */
		sleep(300);

		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);

		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* unlink from cluster A */
		bRet = unlinkCmd();

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		sleep(50);
		
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}

	//@Test
	public void Function_02_duplex_write_independent() {

		boolean bRet = false;

		caseName = "Function_02_duplex_write_independent";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120 s */
		sleep(300);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);

		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* delete from cluster C */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(100);
		
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}

//	@Test
	public void Function_03_sync_while_balance() {

		boolean bRet = false;

		caseName = "Function_03_sync_while_balance";
		log.info(caseName + "===> start");

		/* Clean one ds */
		bRet = cleanOneDs(tfsGrid2);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* sleep 120 s */
		sleep(120);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* Start one ds */
		bRet = startOneDs(tfsGrid2);
		Assert.assertTrue(bRet);
		
		/* wait for balance */
		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(50);

		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
	}
	
	//@Test
	public void Function_04_sync_while_compact() {

		boolean bRet = false;

		caseName = "Function_04_sync_while_compact";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(70);
		Assert.assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(500);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* wait  */
		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(50);

		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
	}

	//@Test
	public void Function_05_sync_while_delete() {

		boolean bRet = false;

		caseName = "Function_05_sync_while_delete";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(300);
		
		/* Modify MinReplication */
		bRet = setMinReplication(BLOCKCOPYCNT-1);
		Assert.assertTrue(bRet);
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(BLOCKCOPYCNT-1);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT-1);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* wait  */
		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(50);

		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
		
		/* Modify MinReplication */
		bRet = setMinReplication(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
	}
	
	//@Test
	public void Function_06_sync_while_copy() {

		boolean bRet = false;

		caseName = "Function_06_sync_while_copy";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
	
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(300);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		sleep(300);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);	
		
		/* Start one ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* wait  */
		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(50);

		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}
	
	//@Test
	public void Function_07_sync_while_shutB_startB() {

		boolean bRet = false;

		caseName = "Function_07_sync_while_shutB_startB";
		log.info(caseName + "===> start");

		/* shut cluster B */
		bRet = tfsGrid2.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120 s */
		sleep(300);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* start clusterB */
		bRet = tfsGrid2.start();
		assertTrue(bRet);
		
		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
              
		sleep(50);
	 
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}
	
//	@Test
	public void Function_08_sync_while_shutBC_startBC() {

		boolean bRet = false;

		caseName = "Function_08_sync_while_shutBC_startBC()";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* shut cluster B */
		bRet = tfsGrid2.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

		sleep(10);
        
		/* check cluster B shut */
		bRet = tfsGrid2.isAlive(); 
		Assert.assertFalse(bRet);

		/* shut cluster C */
		bRet = tfsGrid3.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

		sleep(10);

		/* check cluster C status */
		bRet = tfsGrid3.isAlive(); 
		Assert.assertFalse(bRet);

		/* check cluster A status */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* sleep 50 s */
		sleep(300);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* start cluster B */
		bRet = tfsGrid2.start();
		Assert.assertTrue(bRet);

		/* check cluster B alive */
		bRet = tfsGrid2.isAlive();
		Assert.assertTrue(bRet);

		/* shut cluster C */
		bRet = tfsGrid3.start();
		Assert.assertTrue(bRet);

		/* check cluster C start */
		bRet = tfsGrid3.isAlive();
		Assert.assertTrue(bRet);
   
		sleep(120);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);	
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		sleep(50);

		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}

//	@Test
	public void Function_09_sync_while_block_A_ds_with_B_ns() {
		boolean bRet = false;

		caseName = "Function_09_sync_while_block_A_ds_with_B_ns";
		log.info(caseName + "===> start");

		/* block  A ds with B ns */
		bRet = Proc.portOutputBlock(tfsGrid.getCluster(DSINDEX).getServer(0).getIp(),
				tfsGrid2.getCluster(NSINDEX).getServer(0).getIp(),
				tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());
		assertTrue(bRet);		
	
		/* wait 120s */
		sleep(300);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120 s */
		sleep(120);

		/* stop write */
		bRet = writeCmdStop();
		assertTrue(bRet);
 
		/* netUnblock */
		bRet = Proc.netUnblockBase(tfsGrid.getCluster(DSINDEX).getServer(0).getIp());
		assertTrue(bRet);
		
		/* wait 100 s */
		sleep(100);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from cluster A */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		sleep(50);
	
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}

	// @Test
	public void Function_10_sync_while_restart_ds() {

		boolean bRet = false;

		caseName = "Function_10_sync_while_restart_ds";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(300);

		/* kill one ds of A */
		bRet = killOneDs();
		assertTrue(bRet);
	
		sleep(10);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* restart the killed ds */
		bRet = startOneDs();
		assertTrue(bRet);
		
		sleep(50); //TODO: should be longer?

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
	}
	
//	@Test
	public void Function_11_sync_while_restart_A_ds_force() {

		boolean bRet = false;

		caseName = "Function_11_sync_while_restart_A_ds_force";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(120);

		/* kill one ds of A force*/
		bRet = killOneDsForce();
		assertTrue(bRet);
	
		sleep(10);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* restart the killed ds */
		bRet = startOneDs();
		assertTrue(bRet);
		
		sleep(50); //TODO: should be longer?

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterBIP);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* rename log */
		bRet = mvLogFile(tfsReadClient.getLogs() + caseName, clusterCIP);
		Assert.assertTrue(bRet);
		
	}
	
	@Test
	public void Function_12_happy_132ds_to_141ds_sync() {

		boolean bRet = false;

		caseName = "Function_12_happy_132ds_to_141ds_sync";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* write process */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait120 s */
		sleep(3600);

		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);

		sleep(50);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* unlink from cluster B */
		bRet = unlinkCmd();

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		sleep(50);
		
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* Move the unlink file list */
		bRet = mvUnlinkFile();
		Assert.assertTrue(bRet);
	}
	
	/* make sure the first ds of cluster B(1.3ns) has dataserver(132) and dataserver14 in bin dir*/
	//@Test
	public void Function_13_happy_compatibility() {

		boolean bRet = false;

		caseName = "Function_13_happy_compatibility";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* write process */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait120 s */
		sleep(120);
		
		/* kill one ds of B */
		bRet = killAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* rename file queue */
		bRet = renameFileQueue(tfsGrid2);
		assertTrue(bRet);
		
		/* replace 1.3.2ds with 1.4.1ds */
		bRet = replaceOneDs(tfsGrid2);
		assertTrue(bRet);
		
		/* restart the killed ds of B */
		bRet = startAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		sleep(360);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* unlink from cluster b */
		bRet = unlinkCmd();

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		sleep(50);
		
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* kill one ds of B */
		bRet = killAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		/* restore 1.4.1ds with 1.3.2ds */
		bRet = restoreOneDs(tfsGrid2);
		assertTrue(bRet);
		
		/* start the killed ds of B */
		bRet = startAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
	}
	
	/* make sure the first ds of cluster B(1.3ns) has dataserver(132) and dataserver14 in bin dir*/
	//@Test
	public void Function_14_unhappy_compatibility() {

		boolean bRet = false;

		caseName = "Function_14_unhappy_compatibility";
		log.info(caseName + "===> start");
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* write process */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait120 s */
		sleep(120);
		
		/* block B ds with A ns */
		bRet = Proc.portOutputBlock(tfsGrid2.getCluster(DSINDEX).getServer(0).getIp(),
				tfsGrid.getCluster(NSINDEX).getServer(0).getIp(),
				tfsGrid.getCluster(NSINDEX).getServer(0).getPort());
		assertTrue(bRet);
		
		/* wait120 s */
		sleep(120);	
		
		/* make sure sync retry happened*/
		bRet = chkSecondQueue(tfsGrid2, true);
		assertTrue(bRet);
		
		/* kill one ds of B */
		bRet = killAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* rename file queue */
		bRet = renameFileQueue(tfsGrid2);
		assertTrue(bRet);
		
		/* replace 1.3.2ds with 1.4.1ds */
		bRet = replaceOneDs(tfsGrid2);
		assertTrue(bRet);
		
		/* unblock net */
		bRet = Proc.netUnblockBase(tfsGrid2.getCluster(DSINDEX).getServer(0).getIp());
		assertTrue(bRet);
		
		/* restart the killed ds of B */
		bRet = startAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		/* make sure the old retry sync queue has been deleted */
		bRet = chkSecondQueue(tfsGrid2, false);
		assertTrue(bRet);
		
		sleep(360);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
		
		/* unlink from cluster A */
		bRet = unlinkCmd();

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		sleep(50);
		
		/* set read file list */
		bRet = setReadFileList(UNLINKED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		/* set read file list */
		bRet = setReadFileList(SEED_FILE_LIST_NAME);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* kill one ds of B */
		bRet = killAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
		
		/* restore 1.4.1ds with 1.3.2ds */
		bRet = restoreOneDs(tfsGrid2);
		assertTrue(bRet);
		
		/* start the killed ds of B */
		bRet = startAllDsOneSide(tfsGrid2);
		assertTrue(bRet);
	}
	
	@After
	public void tearDown() {
		boolean bRet = false;

		/* Stop all client process */
		bRet = allCmdStop();
		Assert.assertTrue(bRet);

		/* Move the seed file list */
		bRet = mvSeedFile();
		Assert.assertTrue(bRet);
		
		/* Clean the caseName */
		caseName = "";
	}

	@Before
	public void setUp() {
		boolean bRet = false;

		/* Reset case name */
		caseName = "";

		/* Set the failcount */
		bRet = setAllFailCnt();
		Assert.assertTrue(bRet);

		/* Kill the grid */
		bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

		bRet = tfsGrid2.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		//bRet = tfsGrid3.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		//Assert.assertTrue(bRet);
		
		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);

		/* Clean the log file */
		//bRet = tfsGrid.clean();
		//Assert.assertTrue(bRet);

		bRet = tfsGrid.start();
		Assert.assertTrue(bRet);

		//bRet = tfsGrid2.clean();
		//Assert.assertTrue(bRet);

		bRet = tfsGrid2.start();
		Assert.assertTrue(bRet);

		//bRet = tfsGrid3.clean();
		//Assert.assertTrue(bRet);

		//bRet = tfsGrid3.start();
		//Assert.assertTrue(bRet);

		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	}
}
