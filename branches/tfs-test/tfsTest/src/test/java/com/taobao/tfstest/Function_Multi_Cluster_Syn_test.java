package com.taobao.tfstest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ibm.staf.STAFResult;
import com.taobao.gaia.AppCluster;
import com.taobao.gaia.AppGrid;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.gaia.HelpBase;

/**
 * @author lexin
 */

public class Function_Multi_Cluster_Syn_test extends FailOverBaseCase {
	final public int STATUS_NORMAL = 0;
	final public int STATUS_DELETED = 1;

	final public String clusterAIP = tfsGrid.getCluster(NSINDEX).getServer(0).getVip();
	final public String clusterBIP = tfsGrid2.getCluster(NSINDEX).getServer(0).getVip(); 
	final public String clusterCIP =tfsGrid3.getCluster(NSINDEX).getServer(0).getVip(); 

	final public String clusterAAddr = clusterAIP
	                        + ":" + tfsGrid.getCluster(NSINDEX).getServer(0).getPort();
	final public String clusterBAddr = clusterBIP 
	                        + ":" + tfsGrid2.getCluster(NSINDEX).getServer(0).getPort();
	final public String clusterCAddr = clusterCIP 
	                        + ":" + tfsGrid3.getCluster(NSINDEX).getServer(0).getPort();

	/* Other */
	private List<AppServer> serverList;
	protected static Logger log = Logger.getLogger("Cluster");
	HelpBase helpBase = new HelpBase();

	public boolean check_sync(String targetIP, String clusterAddr, int chkVal) {
		log.info("Start to check_sync:" + targetIP + ", " + clusterAddr);
		ArrayList<String> result = new ArrayList<String>(500);

		String strCmd = "cat ";
		strCmd += TEST_HOME
				+ "/tfsseed_file_list.txt | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";
		boolean bRet = false;

		bRet = helpBase.proStartBase(targetIP, strCmd, result);
		if (bRet == false)
		{
			log.error("Get write file list on cluster B failure!");
			return bRet;
		}
		for (int i = 0; i < result.size(); i++) {
			ArrayList<String> chkResult = new ArrayList<String>(20);
			strCmd = "/home/admin/tfs_bin/bin/tfstool -s ";
			strCmd += clusterAddr;
			strCmd += " -n -i \\\"stat ";
			strCmd += result.get(i) + "\\\"";
			log.info("Executed command is :" + strCmd);
			bRet = Proc.cmdOutBase(targetIP, strCmd, "STATUS", 2, null, chkResult);
			if (bRet == false || chkResult.size() <= 0)
			{
				log.error("Check file on cluster B failure!!!!");
				return bRet;
			}
			log.info("-------------->Executed command result is : " + chkResult.get(0));
			assertEquals(chkVal, Integer.parseInt(chkResult.get(0)));
		}
		return bRet;
	}

	public boolean killMasterNs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		AppServer tempIp = null;
		AppServer tempMaster = null;
		List<AppServer> listNs = tfsAppGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0) == tfsAppGrid.getCluster(NSINDEX).getServer(0)) {
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false)
				return bRet;
			tempIp = listNs.get(0);
			tempMaster = listNs.get(1);
		} else {
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false)
				return bRet;
			tempIp = listNs.get(1);
			tempMaster = listNs.get(0);
		}
			
		return bRet;
	}

	public boolean killSlaveNs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		List<AppServer> listNs = tfsAppGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0) == tfsAppGrid.getCluster(NSINDEX).getServer(0)) {
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false)
				return bRet;
		} else {
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false)
				return bRet;
		}

		

		return bRet;
	}

	public boolean killAllDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Kill all ds start ===>");
		for (int iLoop = DSINDEX; iLoop <= DS_CLUSTER_NUM; iLoop++) {
			AppCluster csCluster = tfsAppGrid.getCluster(iLoop);
			for (int jLoop = 0; jLoop < csCluster.getServerList().size(); jLoop++) {
				AppServer cs = csCluster.getServer(jLoop);
				bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
				if (bRet == false) {
					break;
				}
			}
		}
		log.info("Kill all ds end ===>");
		return bRet;
	}

	public boolean startAllDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Start all ds start ===>");
		for (int iLoop = DSINDEX; iLoop <= DS_CLUSTER_NUM; iLoop++) {
			AppCluster csCluster = tfsAppGrid.getCluster(iLoop);
			for (int jLoop = 0; jLoop < csCluster.getServerList().size(); jLoop++) {
				AppServer cs = csCluster.getServer(jLoop);
				bRet = cs.start();
				if (bRet == false) {
					break;
				}
			}
		}
		log.info("Start all ds end ===>");
		return bRet;
	}
	public boolean startSlaveNs(AppGrid tfsAppGrid) {
	      boolean bRet = false;
	      bRet = tfsAppGrid.getCluster(NSINDEX).getServer(1).start();
	      return bRet;
	   }

	   public boolean startNs(AppGrid tfsAppGrid) {
	      boolean bRet = false;
	      AppServer server = new AppServer();
	      AppServer serverSlave = new AppServer();
	      for (int iLoop = 0; iLoop < tfsGrid.getCluster(NSINDEX).getServerList()
	            .size(); iLoop++) {
	         server = tfsAppGrid.getCluster(NSINDEX).getServer(iLoop);
	         if (server.getIp().equals(MASTERSER.getIp())) {
	            bRet = server.start();
	            if (bRet == false)
	               return bRet;
	         } else {
	            serverSlave = server;
	         }
	      }

	      bRet = serverSlave.start();

	      return bRet;
	   }


	public boolean ClusterBMigrateVip() {
		boolean bRet = false;
		log.info("ClusterBMigrateVip vip start ===>");
		AppServer clusterBMASTERSER = tfsGrid2.getCluster(NSINDEX).getServer(0);
		bRet = HA.setVipMigrateBase(clusterBMASTERSER.getIp(),
				clusterBMASTERSER.getIpAlias(), clusterBMASTERSER.getMacName());
		if (bRet == false) {
			return bRet;
		}

		/* Wait for migrate */
		sleep(MIGRATETIME);

		bRet = HA.chkVipBase(clusterBMASTERSER.getIp(), VIPETHNAME);
		log.info("ClusterBMigrateVip vip end ===>");
		return bRet;
	}

	public boolean killOneDsForce(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Kill one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		log.info("Kill one ds end ===>");
		return bRet;
	}

	public boolean cleanOneDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Clean one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
		if (bRet == false)
			return bRet;

		bRet = cs.clean();
		log.info("Clean one ds end ===>");
		return bRet;
	}

	public boolean startOneDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Start one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.start();
		log.info("Start one ds end ===>");
		return bRet;
	}

	public boolean killOneDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Kill one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
		log.info("Kill one ds end ===>");
		return bRet;
	}

	public boolean setClusterAddr(String nsAddr) {
		boolean bRet = false;
		bRet = conf.confReplaceSingleByPart(CLIENTIP, CLIENTCONF, "public",
				NSIP, nsAddr);
		return bRet;
	}

//	@Test
	public void Function_01_clusterA_sync_clusterB() {

		boolean bRet = false;

		caseName = "Function_01_clusterA_sync_clusterB";
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
		sleep(120);

		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);

		sleep(50);

		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
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
		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
	}

	/*
	 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.��ȺA��д����ݡ� 3.��ȺB���޷�ɾ��ȺAд�����ݡ�
	 */
//	@Test
	public void Function_02_duplex_write_independent() {

		boolean bRet = false;

		caseName = "Function_02_duplex_write_independent";
		log.info(caseName + "===> start");

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120 s */
		sleep(120);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);

		sleep(50);
		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* delete from cluster B */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
//		bRet = checkRateEnd(FAILRATE, UNLINK);
//		Assert.assertTrue(bRet);

		sleep(100);
		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
	}

	/*
	 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.��ȺA��д����ݡ�
	 * 3.��ȺB�з������Ǩ��(д֮ǰ��kill��B��Ⱥ��1̨ds��д��֮������)��
	 * 4.��ȺB�п��Կ�����ȺA����д�����ݣ���ȺA����ɾ��д�����ݣ�����ͬ������ȺB��
	 */
//	@Test
	public void Function_03_sync_while_balance() {

		boolean bRet = false;

		caseName = "Function_03_sync_while_balance";
		log.info(caseName + "===> start");

		/* Clean one ds */
		bRet = cleanOneDs(tfsGrid2);
		Assert.assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(120);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);
		sleep(50);

		/* Start one ds */
		bRet = startOneDs(tfsGrid2);
		Assert.assertTrue(bRet);
		
		/* wait for balance */
		sleep(10);
		
		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
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

		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		assertTrue(bRet);
	}

//	@Test
	public void Function_04_sync_while__shutBC_startBC() {
		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.�رռ�ȺBC 3.��Ӱ�켯ȺA�ķ��ʣ�����ͬ����ʧ�ܣ����ýű���⵽����
		 * 4.�ٴ�������ȺBC��ͬ���ָ��������Ⱥ��
		 */

		boolean bRet = false;

		caseName = "Function_04_sync_while__shutBC_startBC()";
		log.info(caseName + "===> start");
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* shut cluster B */
		bRet = tfsGrid2.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

        	sleep(10);
         	/* check cluster B shut */
	//	bRet = tfsGrid2.isAlive(); 
	//	Assert.assertFalse(bRet);

		/* kill cluster C */
		bRet = tfsGrid3.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

                sleep(10);
		/* check cluster C status */
	//	 bRet = tfsGrid3.isAlive(); 
	//	Assert.assertFalse(bRet);

		/* check cluster A status */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

		/* ���ýű����ͬ��ʧ�� */

		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* sleep 50 s */
		sleep(50);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
               
                /* sleep 10 s */
	         sleep(20);

          	/* start cluster B */
		bRet = tfsGrid2.start();
		Assert.assertTrue(bRet);

		/* sleep 50 s */
		sleep(150);

		/* check cluster B start */
	//	bRet = tfsGrid2.isAlive();
	//	Assert.assertTrue(bRet);

		/* shut cluster C */
		bRet = tfsGrid3.start();
		Assert.assertTrue(bRet);

		/* sleep 50 s */
		sleep(50);

		/* check cluster C start */
	//	bRet = tfsGrid3.isAlive();
	//	Assert.assertTrue(bRet);
                
                
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);
	        sleep(120);

		/* verification */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
	
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
		Assert.assertTrue(bRet);	
		
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

		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);	
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);			


	}
@Test

//	@Test
	public void Function_05_sync_while_shutB_startB() {

		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.�رռ�ȺB 3.��Ӱ�켯ȺA��C�ķ����Ѿ�ͬ���� 2.�ٴ�������ȺB��3����Ⱥ��
		 */

		boolean bRet = false;

		caseName = "Function_05_sync_while_shutB_startB";
		log.info(caseName + "===> start");

		/*kill clusterB*/
		killMasterNs(tfsGrid2);
		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120 s */
		sleep(120);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
                sleep(10);

		/* start clusterB */
		bRet = tfsGrid2.start();
		assertTrue(bRet);
		
		sleep(50);
		bRet = check_sync(MASTERIP,clusterBAddr, STATUS_NORMAL);
		Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		/* verify C */
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
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
	 
        	bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);
	}

//	@Test
	public void Function_06_sync_while_netunblockB() {
		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.����ȺB�뼯ȺC 4.��ȺAд���  5.һ��ʱ�����BC������6����֤���ͬ��
		 */

		boolean bRet = false;

		caseName = "Function_06_sync_while_netunblockB";
		log.info(caseName + "===> start");

		/* block clusterB  net */
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(DSINDEX)
				.getServer(0).getIp(), 1, 5);
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid3.getCluster(DSINDEX)
				.getServer(0).getIp(), 1, 5);
	
		/* wait 120s */
		sleep(120);
		
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

		sleep(50);
 
		/* netUnblock Cluster B and cluster C */
		helpBase.netUnblockBase(MASTERSER.getIp());

		/* wait 100 s */
		sleep(100);
		
		/* verification */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		sleep(10);
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
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
	
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);	
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_DELETED);
		Assert.assertTrue(bRet);		
	}

	// @Test
	public void Function_07_sync_while__del_A_() {
		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.��ȺA��д��ɾ�����3.�鿴ABC�Ƿ�ͬ���������Ⱥ��
		 */
		boolean bRet = false;

		caseName = "Function_07_sync_while__del_A";
		log.info(caseName+"===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100 s */
		sleep(120);

		/* stop write */
		bRet = writeCmdStop();
		assertTrue(bRet);
                sleep(50);
		/* verify */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
		assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
	//	bRet = checkRateEnd(FAILRATE, UNLINK);
	//	Assert.assertTrue(bRet);
                sleep(50);
		/* Check sync */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		assertTrue(bRet);
		
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_DELETED);
		assertTrue(bRet);
	}
	
	//@Test
	public void Function_08_sync_while__shutBC_startBC() {
		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.�رռ�ȺB 3.��Aд����ݣ�
		 * 4.�ٴ�������ȺB��ͬ���ָ��������Ⱥ��
		 */

		boolean bRet = false;

		caseName = "Function_08_sync_while__shutBC_startBC";
		log.info(caseName+"===> start");

		/* shut cluster B */
		killMasterNs(tfsGrid2);
//		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(120);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* start cluster B */
		bRet = tfsGrid2.start();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(50);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* Check block copys */
	//	bRet = chkBlockCntBothNormal(1);
	//	Assert.assertTrue(bRet);

		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);

		/* verification */
	
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
	
		check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
	
	}

//	@Test
	public void Function_09_sync_while_AB_restartDS_Kill_9() {
		/*
		 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.��ȺA��д��ɾ����ݡ� 3.������̨(��̨)DS��kill -9��
		 */
		boolean bRet = false;

		caseName = "Function_08_sync_while_AB_restartDS_Kill_9";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100 s */
		sleep(120);
		
		/* kill one ds of cluster A */
		bRet = killOneDsForce();
		assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* start the killed ds */
		bRet = startOneDs();
		assertTrue(bRet);	
		sleep(50);
		/* Check B ststus */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* Check C ststus */
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
		assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterCAddr);
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
	
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);
		
		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);
		
		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
	       
                sleep(50);	
	 
           	/* Check sync */
		bRet = check_sync(MASTERIP, clusterBAddr, STATUS_DELETED);
		assertTrue(bRet);
		
		/* Check sync */
		bRet = check_sync(MASTERIP, clusterCAddr, STATUS_DELETED);
		assertTrue(bRet);		
		
	}

	// @Test
	public void Function_10_sync_while_restartDS() {
		/*
		 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.��ȺA��д��ɾ����ݡ� 3.������̨(��̨)DS
		 */
		boolean bRet = false;

		caseName = "Function_09_sync_while_restartDS_()";
		log.info(caseName + "===> start");

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		sleep(120);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAAddr, STATUS_NORMAL);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

                /* Monitor the unlink process */
                bRet = unlinkCmdMon();
                Assert.assertTrue(bRet);
                
		/* wait 120 s */
		sleep(120);

		/* Check rate */
	//	bRet = checkRateEnd(FAILRATE, UNLINK);
		//Assert.assertTrue(bRet);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAAddr, STATUS_NORMAL);

		/* kill DS ofB kill_9 */
		killOneDs(tfsGrid2);

		/* wait 100 s */
		sleep(100);

		/* start DS ofB kill_9 */
		startOneDs(tfsGrid2);
 
                sleep(50);
  		
                /* Check AB data */
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);

	}

	/*
	 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.����ȺA�е�ĳ̨ds����ȺB�е�DS 3.��ȺA��д��ɾ����ݡ� 4.һ��ʱ��󣬽����������
	 */
	//@Test
	public void Function_11_sync_while_block_one_ds() {

		boolean bRet = false;

		caseName = "Function_11_sync_while_block_one_ds";
		log.info(caseName + "===> start");

		/* Block clusterB DS net */
		helpBase.portBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getPort());

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100s */
		sleep(120);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* wait 50s */
		sleep(50);
		
		
		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
	//	bRet = checkRateEnd(FAILRATE, UNLINK);
	//	Assert.assertTrue(bRet);
                
                sleep(50);
		/* Check A ststus */
		check_sync(MASTERIP, clusterAAddr, STATUS_NORMAL);

		/* netUnblock Cluster DS */
		helpBase.portOutputBlock(MASTERSER.getIp(), tfsGrid2
				.getCluster(NSINDEX).getServer(0).getIp(),
				tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());

               	sleep(50);
            	/* verification */
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);

	}
	/*
	 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.����ȨA�е�ĳ̨ds����ȺC�е�DS 3.��ȺA��д��ɾ����ݡ� 4.һ��ʱ��󣬽����������
	 */
	// @Test
	public void Function_12_sync_while_block_one_ds() {

		boolean bRet = false;

		caseName = "Function_10_sync_while_block_one_ds()";
		log.info(caseName + "===> start");

		/* Block clusterC DS net */
		helpBase.portBlockBase(MASTERSER.getIp(), tfsGrid3.getCluster(NSINDEX)
				.getServer(0).getIp(), tfsGrid3.getCluster(NSINDEX)
				.getServer(0).getPort());

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120s */
		sleep(120);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
                /* wait 50s */
                sleep(50);
		/* Check rate */
	//	bRet = checkRateEnd(FAILRATE, UNLINK);
	//	Assert.assertTrue(bRet);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAAddr, STATUS_NORMAL);

		/* netUnblock Cluster DS */
		helpBase.portOutputBlock(MASTERSER.getIp(), tfsGrid2
				.getCluster(NSINDEX).getServer(0).getIp(),
				tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());

                /* wait 50s */
                sleep(50);
		
                /* verification */
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);

	}

	/*
	 * 1.����˫��Ⱥ����ȺA�ͼ�ȺB�� 2.����ȨA�е�ĳ̨ds����ȺC�е�nS 3.��ȺA��д��ɾ����ݡ� 4.һ��ʱ��󣬽����������
	 */
	// @Test
	public void Function_13_sync_clusterA_blocknet_ns() {
		
		boolean bRet = false;

		caseName = "Function_13_sync_clusterA_blocknet_ns()";
		log.info(caseName + "===> start");

		
		/* Block clusterB DS net */
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid3.getCluster(NSINDEX)
				.getServer(0).getIp(), 1, 5);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 120s */
		sleep(120);

		/* stop write */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

                /* wait 50 s */
                sleep(50);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* netUnblock Cluster DS */
		helpBase.netUnblockBase(MASTERSER.getIp());

		/* verification */
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
	}
//	@Test
	public void Function_14_sync_while__shutBC_startBC() {
		/*
		 * 1.���ö༯Ⱥ����ȺA�ͼ�ȺB�ͼ�ȺC 2.�رռ�ȺBC 3.��Ӱ�켯ȺA�ķ��ʣ�����ͬ����ʧ�ܣ����ýű���⵽����
		 * 4.�ٴ�������ȺBC��ͬ���ָ��������Ⱥ��
		 */

		boolean bRet = false;

		caseName = "Function_14_sync_while__shutBC_startBC()";
		log.info(caseName + "===> start");

		/* shut cluster B */
		killMasterNs(tfsGrid2);
		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);

		/* check cluster B status */
	//	bRet = tfsGrid2.isAlive(); 
	//	Assert.assertFalse(bRet);

		/* kill cluster C */
		 killMasterNs(tfsGrid3);
		 killSlaveNs(tfsGrid3);
		 killAllDs(tfsGrid3);

		/* check cluster C status */
	//	 bRet = tfsGrid3.isAlive(); 
	//	Assert.assertFalse(bRet);

		/* check cluster A status */
	//	bRet = tfsGrid.isAlive();
	//	Assert.assertTrue(bRet);
		
		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAAddr);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(100);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);
		

        /*�ű���ⲻͬ��*/
		
		/* start cluster B */
		bRet = tfsGrid2.start();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(100);

		/* check cluster B status*/
	//	bRet = tfsGrid2.isAlive();
	//	Assert.assertTrue(bRet);

		/* start cluster C */
		bRet = tfsGrid3.start();
		assertTrue(bRet);	

		/* stop 100 s */
		sleep(100);

		/* check cluster C status */
//		bRet = tfsGrid3.isAlive();
//		Assert.assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
		Assert.assertTrue(bRet);

		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);

		/* verification */
	
		check_sync(MASTERIP, clusterBAddr, STATUS_NORMAL);
	
		check_sync(MASTERIP, clusterCAddr, STATUS_NORMAL);
	
	}

	@After
	public void tearDown() {
		boolean bRet = false;

		/* Stop all client process */
		bRet = allCmdStop();
		Assert.assertTrue(bRet);

		/* Move the seed file list */
		// bRet = mvSeedFile();
		// Assert.assertTrue(bRet);

		/* Clean the caseName */
		caseName = "";
	}

	@Before
	public void setUp() {
		boolean bRet = false;

		/* Reset case name */
		caseName = "";

		if (!grid_started) {
			/* Set the failcount */
			bRet = setAllFailCnt();
			Assert.assertTrue(bRet);

			/* Kill the grid */
			bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
			Assert.assertTrue(bRet);

			bRet = tfsGrid2.stop(KillTypeEnum.FORCEKILL, WAITTIME);
			Assert.assertTrue(bRet);
			
			bRet = tfsGrid3.stop(KillTypeEnum.FORCEKILL, WAITTIME);
			Assert.assertTrue(bRet);
			/* Set Vip */
			bRet = migrateVip();
			Assert.assertTrue(bRet);

			/* Clean the log file */
			bRet = tfsGrid.clean();
			Assert.assertTrue(bRet);

			bRet = tfsGrid.start();
			Assert.assertTrue(bRet);

			bRet = tfsGrid2.clean();
			Assert.assertTrue(bRet);

			bRet = tfsGrid2.start();
			Assert.assertTrue(bRet);

			bRet = tfsGrid3.clean();
			Assert.assertTrue(bRet);

			bRet = tfsGrid3.start();
			Assert.assertTrue(bRet);

			/* Set failcount */
			bRet = resetAllFailCnt();
			Assert.assertTrue(bRet);

			grid_started = true;
		}

	}
}
