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

	final public String clusterAIP = tfsGrid.getCluster(NSINDEX).getServer(0)
			.getVip();
	final public String clusterBIP = tfsGrid2.getCluster(NSINDEX).getServer(0)
			.getVip();
	// final public String clusterCIP =
	// tfsGrid3.getCluster(NSINDEX).getServer(0).getVip();

	/* Other */
	public String caseName = "";
	private List<AppServer> serverList;
	protected static Logger log = Logger.getLogger("Cluster");
	HelpBase helpBase = new HelpBase();

	public boolean check_sync(String clusterAIP, String clusterBIP, int chkVal) {
		log.info("Start to check_sync:" + clusterAIP + ", " + clusterBIP);
		ArrayList<String> result = new ArrayList<String>(500);

		String strCmd = "cat ";
		strCmd += TEST_HOME
				+ "/tfsseed_file_list.txt | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";
		boolean bRet = false;
		bRet = helpBase.proStartBase(clusterAIP, strCmd, result);
		assertTrue("Get write file list on cluster B failure!", bRet);
		for (int i = 0; i < result.size(); i++) {
			// TODO: tfstool usage
			ArrayList<String> chkResult = new ArrayList<String>(20);
			strCmd = "/home/admin/tfs_bin/bin/tfstool -s ";
			strCmd += clusterBIP;
			strCmd += ":3100 -n -i\\\"stat ";
			strCmd += result.get(i) + "\\\"";
			strCmd += " |grep STATUS | awk '{print $2}'";
			log.info("Executed command is :" + strCmd);
			bRet = helpBase.proStartBase(clusterBIP, strCmd, chkResult);
			assertTrue("Check file on cluster B failure!!!!", bRet);
			assertEquals(chkVal, Integer.parseInt(chkResult.get(0)));
		}
		return false;
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

		/* Wait for vip migrate */
		sleep(MIGRATETIME);

		/* Check vip */
		bRet = HA.chkVipBase(tempIp.getIp(), VIPETHNAME);
		if (bRet == true) {
			log.error("VIP is not migrate yet!!!");
			return false;
		}

		/* Reset the failcount */
		bRet = resetFailCount(tempIp);
		if (bRet == false)

			return bRet;
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

		/* Wait for vip migrate */
		sleep(5);

		/* Check vip */
		bRet = HA.chkVipBase(tfsAppGrid.getCluster(NSINDEX).getServer(0)
				.getIp(), VIPETHNAME);
		if (bRet == false) {
			log.error("VIP is not on master ns yet!!!");
			return false;
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

	public boolean startSlaveNs(AppGrid tfsGrid2) {
		boolean bRet = false;
		bRet = SLAVESER.start();
		return bRet;
	}

	public boolean startNs(AppGrid tfsGrid2) {
		boolean bRet = false;
		AppServer server = new AppServer();
		AppServer serverSlave = new AppServer();
		for (int iLoop = 0; iLoop < tfsGrid.getCluster(NSINDEX).getServerList()
				.size(); iLoop++) {
			server = tfsGrid.getCluster(NSINDEX).getServer(iLoop);
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

	public boolean setClusterAddr(String nsIp) {
		boolean bRet = false;
		bRet = conf.confReplaceSingleByPart(CLIENTIP, CLIENTCONF, "public",
				NSIP, nsIp);
		return bRet;
	}

	@Test
	public void Function_01_clusterA_sync_clusterB() {

		boolean bRet = false;

		caseName = "Function_01_clusterA_sync_clusterB";
		log.info(caseName + "===> start");

		/* write to cluster A */
		bRet = setSeedFlag(1);
		assertTrue(bRet);

		bRet = setSeedSize(1);
		assertTrue(bRet);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* write process */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait100 s */
		sleep(100);

		/* stop write proccess */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* unlink cluster A */
		bRet = unlinkCmd();

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_DELETED);
	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。 3.集群B中无法删除集群A写入的数据。
	 */
	// @Test
	public void Function_02_duplex_write_independent() {

		boolean bRet = false;

		caseName = "Function_02_duplex_write_independent";
		log.info(caseName + "===> start");

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100 s */
		sleep(100);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterBIP);
		assertTrue(bRet);

		/* delete from cluster B */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。
	 * 3.集群B中发生数据迁移(集群B一开始需要3台机器，写之前先kill掉1台，写完之后启动)。
	 * 4.集群B中可以看到集群A中已写入的数据，集群A可以删除写入的数据，并且同步到集群B上
	 */
	// @Test
	public void Function_03_sync_while_migrate_block() {

		boolean bRet = false;

		caseName = "Function_03_sync_while_move_block";
		log.info(caseName + "===> start");

		/* Clean one ds */
		bRet = cleanOneDs(tfsGrid2);
		Assert.assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(100);

		/* stop write process */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* Start one ds */
		bRet = startOneDs(tfsGrid2);
		Assert.assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_04_sync_while__shutBC_startBC() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群BC 3.不影响集群A的访问，但是同步会失败（可用脚本检测到）。
		 * 4.再次启动集群BC，同步恢复（3个集群）
		 */

		boolean bRet = false;

		caseName = " Function_04_sync_while__shutBC_startBC()";
		log.info(caseName + "===> start");

		/* shut cluster B */
		killMasterNs(tfsGrid2);
		killSlaveNs(tfsGrid2);
		killAllDs(tfsGrid2);

		/* check cluster B shut */
		bRet = chkAlive();
		Assert.assertFalse(bRet);

		/* shut cluster C */
		// killMasterNs(tfsGrid3);
		// killSlaveNs(tfsGrid3);
		// killAllDs(tfsGrid3);

		/* check cluster C shut */
		bRet = chkAlive();
		Assert.assertFalse(bRet);

		/* check cluster A work normal */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

		/* 调用脚本检测同步失败 */

		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
		assertTrue(bRet);

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		/* stop 100 s */
		sleep(100);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* start cluster B */
		startNs(tfsGrid2);
		startSlaveNs(tfsGrid2);
		startAllDs(tfsGrid2);

		/* stop 100 s */
		sleep(100);

		/* check cluster B start */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

		/* shut cluster C */
		// startNs(tfsGrid3);
		// startSlaveNs(tfsGrid3);
		// startAllDs(tfsGrid3);
		//
		/* stop 100 s */

		/* stop 100 s */
		sleep(100);

		/* check cluster C start */
		bRet = chkAlive();
		Assert.assertTrue(bRet);

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
		check_sync(MASTERIP, clusterAIP, STATUS_NORMAL);
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_05_sync_while_shutB_startB() {

		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群B 3.不影响集群A和C的访问已经同步。 2.再次启动集群B（3个集群）
		 */

		boolean bRet = false;

		caseName = " Function_05_sync_while_shutB_startB()";
		log.info(caseName + "===> start");

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

		/* wait 100 s */
		sleep(100);

		/* Stop write cmd */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* Wait 100s for recover cluster B */
		sleep(100);

		/* start clusterB */
		startNs(tfsGrid2);
		startAllDs(tfsGrid2);
		startSlaveNs(tfsGrid2);

		/* verify A is sync with B&C */
		// check_sync(MASTERIP, clusterCIP, STATUS_NORMAL);
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_06_sync_while_netunblockB_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.阻塞集群B与集群C 3.一段时间后解除阻塞
		 */

		boolean bRet = false;

		caseName = " Function_06_sync_while_netunblockB_()";
		log.info(caseName + "===> start");

		/* block clusterB net */
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getIp(), 1, 5);

		/* wait 100s */
		sleep(100);

		/* netUnblock Cluster B and cluster C */
		helpBase.netUnblockBase(MASTERSER.getIp());

		/* verification */

		// check_sync(MASTERIP, clusterCIP, STATUS_NORMAL);
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_07_sync_while__del_A_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.集群A中写入删除数据（3个集群）
		 */
		boolean bRet = false;

		caseName = " Function_07_sync_while__del_A_()";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100 s */
		sleep(100);

		/* stop write */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* wait 100 s */
		sleep(100);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* verify */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

		/* Check ABC sysnc */
		// check_sync(MASTERIP, clusterCIP, STATUS_NORMAL);
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_08_sync_while_AB_restartDS_Kill_9() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS（kill -9）
		 */
		boolean bRet = false;

		caseName = " Function_08_sync_while_AB_restartDS_Kill_9()";
		log.info(caseName + "===> start");

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		sleep(100);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAIP, STATUS_NORMAL);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* start DS ofB kill_9 */
		killOneDsForce(tfsGrid2);
		startOneDs(tfsGrid2);

		/* Check AB data */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
	}

	// @Test
	public void Function_09_sync_while_restartDS() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS
		 */
		boolean bRet = false;

		caseName = " Function_09_sync_while_restartDS_()";
		log.info(caseName + "===> start");

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		sleep(100);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAIP, STATUS_NORMAL);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertTrue(bRet);

		/* wait 100 s */
		sleep(100);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAIP, STATUS_NORMAL);

		/* kill DS ofB kill_9 */
		killOneDs(tfsGrid2);

		/* wait 100 s */
		sleep(100);

		/* start DS ofB kill_9 */
		startOneDs(tfsGrid2);

		/* Check AB data */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的DS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	// @Test
	public void Function_10_sync_while_block_one_ds() {

		boolean bRet = false;

		caseName = " Function_10_sync_while_block_one_ds()";
		log.info(caseName + "===> start");

		/* Block clusterB DS net */
		helpBase.portBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getPort());

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* write to cluster A */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 100s */
		sleep(100);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* wait 50s */
		sleep(50);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* Check A ststus */
		check_sync(MASTERIP, clusterAIP, STATUS_NORMAL);

		/* netUnblock Cluster DS */
		helpBase.portOutputBlock(MASTERSER.getIp(), tfsGrid2
				.getCluster(NSINDEX).getServer(0).getIp(),
				tfsGrid2.getCluster(NSINDEX).getServer(0).getPort());

		/* verification */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的nS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	// @Test
	public void clusterA_block_clusterB_ns() {
		boolean bRet = false;

		/* block clusterB net */
		/* Block clusterB DS net */
		helpBase.netBlockBase(MASTERSER.getIp(), tfsGrid2.getCluster(NSINDEX)
				.getServer(0).getIp(), 1, 5);

		/* set write/read/unlink cluster addr */
		bRet = setClusterAddr(clusterAIP);
		assertTrue(bRet);

		/* write to cluster A */

		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 50s */
		sleep(50);

		/* stop write */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = unlinkCmd();
		assertFalse(bRet);

		/* wait 100 s */
		sleep(100);

		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);

		/* Check rate */
		bRet = checkRateEnd(FAILRATE, UNLINK);
		Assert.assertTrue(bRet);

		/* netUnblock Cluster DS */
		helpBase.netUnblockBase(MASTERSER.getIp());

		/* verification */
		check_sync(MASTERIP, clusterBIP, STATUS_NORMAL);
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

			/* Set failcount */
			bRet = resetAllFailCnt();
			Assert.assertTrue(bRet);

			grid_started = true;
		}

	}
}