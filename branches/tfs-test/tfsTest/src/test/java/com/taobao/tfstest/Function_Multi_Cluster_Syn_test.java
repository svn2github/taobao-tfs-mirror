package com.taobao.tfstest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
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
	/* Other */
	public String caseName = "";
	private List<AppServer> serverList;
	protected static Logger log = Logger.getLogger("Cluster");
    HelpBase helpBase = new HelpBase();
	/**
	 * 
	 * @param sec
	 */
	public void sleep(int sec) {
		log.debug("wait for " + sec + "s");
		try {
			Thread.sleep(sec * 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean check_sync(String clusterAIP, String clusterBIP) {
		ArrayList<String> result = new ArrayList<String>(500);

		String strCmd = "cat /home/admin/tfstest_new/dsExt3/tfsseed_file_list.txt | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";// hava problem
		boolean bRet = false;
		bRet = helpBase.proStartBase(clusterAIP, strCmd, result);
		assertTrue("Get write file list on cluster B failure!", bRet);
		for (int i = 0; i < result.size(); i++) {
			// TODO: tfstool usage
			strCmd = "./tfstool stat" + result.get(i);
			bRet = helpBase.proStartBase(clusterBIP, strCmd);
			assertTrue("Check file on cluster B failure!", bRet);
		}
		return false;
	}

	@Test
	public void clusterA_sync_clusterB() {
		boolean bRet = false;

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);
		bRet = setSeedSize(1);
		assertTrue(bRet);
		
		/*stop write process*/
		bRet = writeCmd();
		assertTrue(bRet);

		/*wait 20 s*/
		sleep(20);
		
		/*stop write proccess*/
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* verify */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());

		/* unlink to cluster A */
		bRet=unlinkCmd();

		/* verify */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。 3.集群B中不能删除集群A写入的数据。
	 */
	@Test
	public void clusterA_shall_not_effect_clusterB() {
		boolean bRet = false;

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);
		bRet = setSeedSize(1);
		assertTrue(bRet);
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 5 s */
		sleep(5);
		
		/*stop write process*/
		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* delete from cluster B */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* verify */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}


	public boolean ClusterBMigrateVip()
	{
		boolean bRet = false;
		log.info("ClusterBMigrateVip vip start ===>");
		AppServer clusterBMASTERSER = tfsGrid2.getCluster(NSINDEX).getServer(0);
		bRet = HA.setVipMigrateBase(clusterBMASTERSER.getIp(), 
		                            clusterBMASTERSER.getIpAlias(), 
		                            clusterBMASTERSER.getMacName());
		if (bRet ==  false)
		{
			return bRet;
		}

		/* Wait for migrate */
		sleep(MIGRATETIME);
		
		bRet = HA.chkVipBase(clusterBMASTERSER.getIp(), VIPETHNAME);
		log.info("ClusterBMigrateVip vip end ===>");
		return bRet;	
	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。 3.集群B中发生迁移。 4.集群A可以删除写入的数据。并且同步到集群B上
	 */
	@Test
	public void Cluster_A_write_B_migrate() {
		boolean bRet = false;
		caseName = " Cluster_A_write_B_move";
		log.info(caseName + "===> start");
		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);
		
		/*stop 20 s*/
		sleep(20);
        bRet = writeCmdStop();
       
		/* Migrate vip */
		ClusterBMigrateVip();

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}


	public boolean killMasterNs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		AppServer tempIp = null;
		AppServer tempMaster = null;
		List<AppServer> listNs = tfsAppGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0) == tfsAppGrid.getCluster(NSINDEX).getServer(0))
		{
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
			tempIp = listNs.get(0);
			tempMaster = listNs.get(1);
		} else {
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
			tempIp = listNs.get(1);
			tempMaster = listNs.get(0);
		}
		
		/* Wait for vip migrate */
		sleep (MIGRATETIME);
		
		/* Check vip */
		bRet = HA.chkVipBase(tempIp.getIp(), VIPETHNAME);
		if (bRet == true)
		{
			log.error("VIP is not migrate yet!!!");
			return false;
		}
		
		/* Reset the failcount */
		bRet = resetFailCount(tempIp);
		if (bRet == false) return bRet;

		return bRet;
	}
	public boolean killSlaveNs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		List<AppServer> listNs = tfsAppGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0) == tfsAppGrid.getCluster(NSINDEX).getServer(0))
		{
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		} else {
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		}
		
		/* Wait for vip migrate */
		sleep (5);
		
		/* Check vip */
		bRet = HA.chkVipBase(tfsAppGrid.getCluster(NSINDEX).getServer(0).getIp(), VIPETHNAME);
		if (bRet == false)
		{
			log.error("VIP is not on master ns yet!!!");
			return false;
		}
		
		return bRet;
	}
	public boolean killAllDs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		log.info("Kill all ds start ===>");
		for (int iLoop = DSINDEX; iLoop <= DS_CLUSTER_NUM; iLoop ++)
		{
			AppCluster csCluster = tfsAppGrid.getCluster(iLoop);
			for(int jLoop = 0; jLoop < csCluster.getServerList().size(); jLoop ++)
			{
				AppServer cs = csCluster.getServer(jLoop);
				bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
				if (bRet == false)
				{
					break;
				}
			}
		}
		log.info("Kill all ds end ===>");
		return bRet;
	}
	public boolean startAllDs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		log.info("Start all ds start ===>");
		for (int iLoop = DSINDEX; iLoop <= DS_CLUSTER_NUM; iLoop++)
		{
			AppCluster csCluster = tfsAppGrid.getCluster(iLoop);
			for(int jLoop = 0; jLoop < csCluster.getServerList().size(); jLoop++)
			{
				AppServer cs = csCluster.getServer(jLoop);
				bRet = cs.start();
				if (bRet == false)
				{
					break;
				}
			}
		}
		log.info("Start all ds end ===>");
		return bRet;
	}
	
	public boolean startSlaveNs(AppGrid tfsGrid2)
	{
		boolean bRet = false;
		bRet = SLAVESER.start();
		return bRet;
	}
	
	public boolean startNs(AppGrid tfsGrid2)
	{
		boolean bRet = false;
		AppServer server = new AppServer();
		AppServer serverSlave = new AppServer();
		for(int iLoop = 0; iLoop < tfsGrid.getCluster(NSINDEX).getServerList().size(); iLoop ++)
		{
			server = tfsGrid.getCluster(NSINDEX).getServer(iLoop);
			if(server.getIp().equals(MASTERSER.getIp()))
			{
				bRet = server.start();
				if (bRet == false) return bRet;
			} else {
				serverSlave = server;
			}
		}
		
		bRet = serverSlave.start();
		
		return bRet;
	}


	@Test
	public void Cluster_ClusterABC_BC_shut() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群BC 3.不影响集群A的访问以及同步。 4.再次启动集群BC
		 */

		boolean bRet = false;
		caseName = " Cluster_ABC_write_BC_shut";
		log.info(caseName + "===> start");

		killMasterNs(tfsGrid2);
		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);

		/* cluster A work normal */
		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);
		
		/*stop 20 s*/
		sleep(20);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
		Assert.assertTrue(bRet);

		/* Check block copy count */
		bRet = chkBlockCopyCnt();
		Assert.assertTrue(bRet);

		/* Check vip */
		bRet = chkVip();
		Assert.assertTrue(bRet);

		/* Check the status of servers */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

		/* start clusterB and clusterC */
		startNs(tfsGrid2);
		startAllDs(tfsGrid2);

		/* verification */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}

	@Test
	public void Cluster_ABC_shutB_startB() {

		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群B 3.不影响集群A和C的访问已经同步。 2.再次启动集群B
		 */

		boolean bRet = false;

		killMasterNs(tfsGrid2);
		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1000);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 10 s */
		sleep(10);
		
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* Wait 20s for recover cluster B */
		sleep (20);
		
		/* start clusterB */
		startNs(tfsGrid2);
		startAllDs(tfsGrid2);
		startSlaveNs(tfsGrid2);
		
		/* verify A is sync with B&C */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}

	@Test
	public void Cluster_ABC_shutB_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.阻塞集群B与集群C 3.一段时间后解除阻塞
		 */
		/* block clusterB net */
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(NSINDEX).getServer(0).getIp(), 1, 5);

		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster B and cluster C */
		helpBase.netUnblockBase(MASTERSER.getIp());

		/* verification */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}

	@Test
	public void Cluster_ABC_del_A_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.集群A中写入删除数据
		 */
		boolean bRet = false;

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1000);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 10 s */
		sleep(100);

		bRet = writeCmdStop();
		assertTrue(bRet);
		
		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* wait 10 s */
		sleep(10);
		
		bRet = unlinkCmdMon();
		assertTrue(bRet);
		
		/* ssm Check ABC data */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());
	}

	public boolean killOneDsForce(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		log.info("Kill one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		log.info("Kill one ds end ===>");
		return bRet;
	}
	public boolean startOneDs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		log.info("Start one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.start();
		log.info("Start one ds end ===>");
		return bRet;
	}
	public boolean killOneDs(AppGrid tfsAppGrid)
	{
		boolean bRet = false;
		log.info("Kill one ds start ===>");
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
		log.info("Kill one ds end ===>");
		return bRet;
	}

	@Test
	public void Cluster_AB_restartDS_Kill_9() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS（kill -9）
		 */
		boolean bRet = false;

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

        sleep(10);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* start DS ofB kill_9 */
		killOneDsForce(tfsGrid2);
		startOneDs(tfsGrid2);

		/* Check AB data */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());

	}

	@Test
	public void Cluster_AB_restartDS() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS
		 */
		boolean bRet = false;

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

        sleep(10);
		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* start DS ofB kill_9 */
		killOneDs(tfsGrid2);
		startOneDs(tfsGrid2);

		/* Check AB data */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的DS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	@Test
	public void clusterA_block_clusterB_one_ds() {
		boolean bRet = false;
		/* block clusterB net */
		String serverIPA = "10.232.4.11";
		String serverIPB = "10.232.4.12";

		/* Block clusterB DS net */
		helpBase.portBlockBase(MASTERSER.getIp(), 
		              tfsGrid2.getCluster(NSINDEX).getServer(0).getIp(), 
		              tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1000);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);
		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster DS */
		helpBase.portOutputBlock(MASTERSER.getIp(), 
		                tfsGrid2.getCluster(NSINDEX).getServer(0).getIp(), 
		                tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());

		/* verification */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的nS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	@Test
	public void clusterA_block_clusterB_ns() {
		boolean bRet = false;

		/* block clusterB net */
		/* Block clusterB DS net */
		helpBase.netBlockBase(MASTERSER.getIp(), 
		            tfsGrid2.getCluster(NSINDEX).getServer(0).getIp(), 1, 5);

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 5s */
		sleep(15);
		
		/*stop write*/
		bRet =writeCmdStop();
		assertTrue(bRet);
		
		
		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);
		
		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster DS */
		helpBase.netUnblockBase(MASTERSER.getIp());
		
		/* verification */
		check_sync(MASTERIP, tfsGrid2.getCluster(NSINDEX).getServer(0).getIp());

	}
}