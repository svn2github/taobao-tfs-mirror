/**
 * 
 */
package com.taobao.tfstest;

import java.util.HashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author mingyan
 *
 */
public class Function_ns_plan_test extends NameServerPlanTestCase {
	
	@Test
	public void Function_01_one_ds_out_emerge_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_01_one_ds_out_emerge_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 10s for recover */
		sleep (10);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_EMERG_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	/* Call setNsConf @setUp to make sure min/maxReplication are both configured as 1 */
	public void Function_02_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_02_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
	
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Modify MaxReplication */
		bRet = setMaxReplication(3);
		Assert.assertTrue(bRet);
		
		/* Wait 10s for copy */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
			
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_03_move_block(){
		
		boolean bRet = false;
		caseName = "Function_03_move_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(100);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Wait 120s for write */
		sleep (120);

		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_MOVE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_04_compact_block(){
		
		boolean bRet = false;
		caseName = "Function_04_compact_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Compact block */
		bRet = compactBlock();
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_COMPACT, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_05_delete_block(){
		
		boolean bRet = false;
		caseName = "Function_05_delete_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Make sure now all blocks have 2 copies */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of replication */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);
		
		/* Start the killed ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of deletion */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 3);
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_DELETE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	/* Call setNsConf @setUp to make sure min/maxReplication are both configured as 1  */
	public void Function_06_emerg_rep_vs_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_06_emerg_rep_vs_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(3);
		Assert.assertTrue(bRet);
		
		/* Modify MinReplication */
		bRet = setMinReplication(2);
		Assert.assertTrue(bRet);

		/* Check REPLICATE plan */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds to clear the plan list */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(PLAN_CHK_NUM, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_07_emerg_rep_vs_move_block(){
		
		boolean bRet = false;
		caseName = "Function_07_emerg_rep_vs_move_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(100);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Check MOVE plan */
		bRet = checkPlan(PlanType.PLAN_TYPE_MOVE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds to clear the plan list */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(PLAN_CHK_NUM, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_08_emerg_rep_vs_compact_block(){
		
		boolean bRet = false;
		caseName = "Function_08_emerg_rep_vs_compact_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Compact block */
		bRet = compactBlock();
		Assert.assertTrue(bRet);
		
		/* Check COMPACT plan */
		bRet = checkPlan(PlanType.PLAN_TYPE_COMPACT, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds to clear the plan list and trigger emerg_rep*/
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(PLAN_CHK_NUM, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_09_emerg_rep_vs_delete_block(){
		
		boolean bRet = false;
		caseName = "Function_09_emerg_rep_vs_delete_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Make sure now all blocks have 2 copies */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of replication */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);
		
		/* Start the killed ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Check DELETE plan */
		bRet = checkPlan(PlanType.PLAN_TYPE_DELETE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Kill the 2nd ds to clear the plan list and trigger emerg_rep*/
		bRet = killSecondDs();
		Assert.assertTrue(bRet);
		
		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(PLAN_CHK_NUM, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_10_one_ds_out_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_10_one_ds_out_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(1);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_11_one_ds_join_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_11_one_ds_join_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);

		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(1);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	public void Function_12_one_ds_restart_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_12_one_ds_restart_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(10);	
		
		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
	
		/* Wait */
		sleep(10);	
		
		/* Check interrupt */
		bRet = checkInterrupt(2);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}

	public void Function_13_new_elect_writable_block_algorithm(){
		
		boolean bRet = false;
		caseName = "Function_13_new_elect_writable_block_algorithm";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Get the block num hold by each ds before write */
		HashMap<String, Integer> blockDisBefore = new HashMap<String, Integer>();
		bRet = getBlockDistribution(blockDisBefore);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Wait for completion of replication */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);
		
		/* Get the block num hold by each ds after write */
		HashMap<String, Integer> blockDisAfter = new HashMap<String, Integer>();
		bRet = getBlockDistribution(blockDisAfter);
		Assert.assertTrue(bRet);
		
		/* Check new added block balance status*/
		bRet = checkWriteBalanceStatus(blockDisBefore, blockDisAfter);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	/* Call setNsConf @setUp to make sure min/maxReplication are configured as 2 & 3  */
	public void Function_14_copy_2_3_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_14_copy_2_3_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCK_CHK_TIME, 1);
		Assert.assertTrue(bRet);	
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 10s for recover */
		sleep (10);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	public void Function_15_modify_gc_wait_time(){
		
		boolean bRet = false;
		caseName = "Function_15_modify_gc_wait_time";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);
		
		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(1800);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(7200);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(3600);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	@After
	public void tearDown(){
		boolean bRet = false;
		
		/* Move the seed file list */
//		bRet = mvSeedFile();
//		Assert.assertTrue(bRet);
		
		/* Clean the caseName */
		caseName = "";
	}
	
	@Before
	public void setUp(){
		boolean bRet = false;
		
		/* Reset case name */
		caseName = "";
		
		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);
		
		/* Kill the grid */
		bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);
		
		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);
		
		/* Clean the log file */
		bRet = tfsGrid.clean();
		Assert.assertTrue(bRet);
		
		/* Set NS conf */
		bRet = setNsConf("nameserver", "max_replication", "3");
		Assert.assertTrue(bRet);
		bRet = setNsConf("nameserver", "min_replication", "2");
		Assert.assertTrue(bRet);
		
		bRet = tfsGrid.start();
		Assert.assertTrue(bRet);
		
		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	
	}
	
}
