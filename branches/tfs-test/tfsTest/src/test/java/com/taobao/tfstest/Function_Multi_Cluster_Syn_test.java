package com.taobao.tfstest;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.ibm.staf.STAFResult;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.tfstest.TFSClusterBasecaseForLX;
import com.taobao.gaia.HelpBase;

/**
 * @author lexin
 */
public class Function_Multi_Cluster_Syn_test extends HelpBase {
	/* Other */
	public String caseName = "";
	private List<AppServer> serverList;
	protected static Logger log = Logger.getLogger("Cluster");

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

	public boolean check_sync(TFSClusterBasecaseForLX clusterA,
			TFSClusterBasecaseForLX clusterB) {
		String strCmd = "cat /home/admin/tfstest_new/dsExt3/tfsseed_file_list.txt | | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";// hava problem
																														
		// String strCmd = "cat /home/admin/tfstest_new/tfsseed_file_list.txt | | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";
		STAFResult result = executeShell(stafhandle, clusterA.MASTERIP, strCmd);
		String[] strResult = result.result.split("\n");
		for (int i = 0; i < strResult.length; i++) {
			// TODO: tfstool usage
			strCmd = "./tfstool stat" + strResult[i];
			STAFResult clusterBResult = executeShell(stafhandle,
					clusterB.MASTERIP, strCmd);
			assertEquals(0, clusterBResult.rc);
		}
		return false;
	}

	@Test
	public void clusterA_sync_clusterB() {
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);
		bRet = clusterA.setSeedSize(1);
		assertTrue(bRet);
		
		/*wait 20 s*/
		sleep(20);
		
		/*stop write process*/
		bRet = clusterA.writeCmd();
		assertTrue(bRet);
		
		/*wait 20 s*/
		sleep(20);
		
		/*stop write proccess*/
		bRet = clusterA.writeCmdStop();
		assertTrue(bRet);
		
		/* verify */
		check_sync(clusterA, clusterB);

		/* unlink to cluster A */
		bRet=clusterA.unlinkCmd();

		/* verify */
		check_sync(clusterA, clusterB);
	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。 3.集群B中不能删除集群A写入的数据。
	 */
	@Test
	public void clusterA_shall_not_effect_clusterB() {
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		/* write to cluster A */

		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);
		bRet = clusterA.setSeedSize(1);
		assertTrue(bRet);
		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* wait 5 s */
		sleep(5);
		
		/*stop write process*/
		bRet = clusterA.writeCmdStop();
		assertTrue(bRet);
		
		/* delete from cluster B */
		bRet = clusterB.unlinkCmd();
		assertFalse(bRet);

		/* verify */
		check_sync(clusterA, clusterB);
	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.集群A中写入数据。 3.集群B中发生迁移。 4.集群A可以删除写入的数据。并且同步到集群B上
	 */
	@Test
	public void Cluster_A_write_B_migrate() {

		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		boolean bRet = false;
		caseName = " Cluster_A_write_B_move";
		log.info(caseName + "===> start");
		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);
		
		/*stop 20 s*/
		sleep(20);
       bRet=clusterA.writeCmdStop();
       
		/* Migrate vip */
		clusterB.migrateVip();

		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertTrue(bRet);

		/* verify */
		check_sync(clusterA, clusterB);
	}

	@Test
	public void Cluster_ClusterABC_BC_shut() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群BC 3.不影响集群A的访问以及同步。 4.再次启动集群BC
		 */

		boolean bRet = false;
		caseName = " Cluster_ABC_write_BC_shut";
		log.info(caseName + "===> start");
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterC = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		clusterB.killMasterNs();
		clusterB.killSlaveNs();
		clusterB.killAllDs();

		clusterC.killMasterNs();
		clusterC.killSlaveNs();
		clusterC.killAllDs();

		/* cluster A work normal */
		/* Check block copys */
		bRet = clusterA.chkBlockCntBothNormal(1);
		assertTrue(bRet);
		
		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);
		
		/*stop 20 s*/
		sleep(20);
		
		/* Stop write cmd */
		bRet = clusterA.writeCmdStop();
		assertTrue(bRet);

		/* Read file */
		bRet = clusterA.chkFinalRetSuc();
		Assert.assertTrue(bRet);

		/* Check block copys */
		bRet = clusterA.chkBlockCntBothNormal(1);
		Assert.assertTrue(bRet);

		/* Check block copy count */
		bRet = clusterA.chkBlockCopyCnt();
		Assert.assertTrue(bRet);

		/* Check vip */
		bRet = clusterA.chkVip();
		Assert.assertTrue(bRet);

		/* Check the status of servers */
		bRet = clusterA.chkAlive();
		Assert.assertTrue(bRet);

		/* start clusterB and clusterC */
		clusterB.startNs();
		clusterB.startAllDs();
		clusterB.startSlaveNs();

		clusterC.startAllDs();
		clusterC.startNs();
		clusterC.startSlaveNs();

		/* verification */
		check_sync(clusterA, clusterB);
		check_sync(clusterA, clusterC);
	}

	@Test
	public void Cluster_ABC_shutB_startB() {

		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.关闭集群B 3.不影响集群A和C的访问已经同步。 2.再次启动集群B
		 */

		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterC = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		clusterB.killMasterNs();
		clusterB.killSlaveNs();
		clusterB.killAllDs();

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1000);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* wait 10 s */
		sleep(100);
		
		bRet = clusterA.writeCmdStop();
		assertTrue(bRet);

		/* verify A is sync with C */
		check_sync(clusterA, clusterC);

		/* Wait 20s for recover cluster B */
		sleep (20);
		
		/* start clusterB */
		clusterB.startNs();
		clusterB.startAllDs();
		clusterB.startSlaveNs();
		
		/* verify A is sync with B&C */
		check_sync(clusterA, clusterB);
		check_sync(clusterA, clusterC);
	}

	@Test
	public void Cluster_ABC_shutB_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.阻塞集群B与集群C 3.一段时间后解除阻塞
		 */
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterC = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		/* block clusterB net */
		String serverIPA = "10.232.4.11";
		String serverIPB = "10.232.4.12";
		String serverIPC = "10.232.4.13";

		/* Block clusterB&C net */
		netBlockBase(serverIPA, serverIPB, 1, 5);
		netBlockBase(serverIPA, serverIPC, 1, 5);

		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster B and cluster C */
		netUnblockBase(serverIPA);
		netUnblockBase(serverIPC);

		/* verification */
		check_sync(clusterA, clusterB);
		check_sync(clusterA, clusterC);
	}

	@Test
	public void Cluster_ABC_del_A_() {
		/*
		 * 1.配置多集群。集群A和集群B和集群C 2.集群A中写入删除数据
		 */
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterC = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1000);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* wait 10 s */
		sleep(100);
		
		bRet = clusterA.writeCmdStop();
		assertTrue(bRet);
		
		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertFalse(bRet);

		/* wait 10 s */
		sleep(100);
		
		bRet = clusterA.unlinkCmdMon();
		assertTrue(bRet);
		
		/* ssm Check ABC data */
		check_sync(clusterA, clusterB);
		check_sync(clusterA, clusterC);
	}

	@Test
	public void Cluster_AB_restartDS_Kill_9() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS（kill -9）
		 */
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1000);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertFalse(bRet);

		/* start DS ofB kill_9 */
		clusterA.killOneDsForce();
		clusterA.startOneDs();

		/* Check AB data */
		check_sync(clusterA, clusterB);

	}

	@Test
	public void Cluster_AB_restartDS() {
		/*
		 * 1.配置双集群。集群A和集群B。 2.集群A中写入删除数据。 3.重启单台(多台)DS
		 */
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1000);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertFalse(bRet);

		/* start DS ofB kill_9 */
		clusterA.killOneDs();
		clusterA.startOneDs();

		/* Check AB data */
		check_sync(clusterA, clusterB);

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的DS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	@Test
	public void clusterA_block_clusterB_one_ds() {
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		/* block clusterB net */
		String serverIPA = "10.232.4.11";
		String serverIPB = "10.232.4.12";

		/* Block clusterB DS net */
		portBlockBase(serverIPA, serverIPB, 3100);

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1000);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertFalse(bRet);
		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster DS */
		portOutputBlock(serverIPA, serverIPB, 3100);

		/* verification */
		check_sync(clusterA, clusterB);

	}

	/*
	 * 1.配置双集群。集群A和集群B。 2.阻塞集权A中的某台ds到集群B中的nS 3.集群A中写入删除数据。 4.一段时间后，解除网络阻塞。
	 */
	@Test
	public void clusterA_block_clusterB_ns() {
		boolean bRet = false;
		TFSClusterBasecaseForLX clusterA = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		TFSClusterBasecaseForLX clusterB = new TFSClusterBasecaseForLX(
				"tfsServer.xml");
		/* block clusterB net */
		String serverIPA = "10.232.4.11";
		String serverIPB = "10.232.4.12";

		/* Block clusterB DS net */
		netBlockBase(serverIPA, serverIPB, 1, 5);

		/* write to cluster A */
		bRet = clusterA.setSeedFlag(1);
		assertTrue(bRet);

		bRet = clusterA.setSeedSize(1);
		assertTrue(bRet);

		bRet = clusterA.writeCmd();
		assertTrue(bRet);

		/* wait 5s */
		sleep(5);
		 /*stop write*/
		bRet =clusterA.writeCmdStop();
		assertTrue(bRet);
		
		
		/* delete from clusterA */
		bRet = clusterA.unlinkCmd();
		assertFalse(bRet);
		
		/* wait 5s */
		sleep(5);

		/* netUnblock Cluster DS */
		netUnblockBase(serverIPA);

		/* verification */
		check_sync(clusterA, clusterB);

	}
}