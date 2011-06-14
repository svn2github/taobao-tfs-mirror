/**
 * 
 */
package com.taobao.tfstest;

import java.util.ArrayList;

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
	
	/* Check dump plan limit */
	final public int DEFAULT_CHECK_PLAN_TIME_LIMIT	= 100;
	final public int DEFAULT_CHECK_PLAN_NUM      	= 20;
	
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
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_EMERG_REPLICATE, DEFAULT_CHECK_PLAN_TIME_LIMIT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
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
	
	/* Make sure minReplication and maxReplication are both configured as 1 */
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
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(3);
		Assert.assertTrue(bRet);
		
		/* Wait 10s for copy */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, DEFAULT_CHECK_PLAN_TIME_LIMIT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
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

	//TODO:check balance_max_diff_block_num modification work?
	public void Function_03_move_block(){
		
		boolean bRet = false;
		caseName = "Function_03_copy_block";
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
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Wait 10s for move */
		sleep (10);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_MOVE, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		bRet = checkPlan(PlanType.PLAN_TYPE_COMPACT, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait for completion of replication*/
		sleep(10);
		
		/* Start the killed ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait for completion of deletion*/
		sleep(10);		
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_DELETE, DEFAULT_CHECK_PLAN_TIME_LIMIT);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	/* Make sure minReplication and maxReplication are both configured as 1 */
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
		
		/* Wait for replication*/
		sleep(10);	
		
		/* Kill the 1st ds to clear the plan list */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(DEFAULT_CHECK_PLAN_NUM, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Wait for replication*/
		sleep(10);	
		
		/* Kill the 1st ds to clear the plan list */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(DEFAULT_CHECK_PLAN_NUM, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		
		/* Wait for replication*/
		sleep(10);	
		
		/* Kill the 1st ds to clear the plan list and trigger emerg_rep*/
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(DEFAULT_CHECK_PLAN_NUM, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait for completion of replication*/
		sleep(10);
		
		/* Start the killed ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait for completion of deletion*/
		sleep(10);		
		
		/* Kill the 2nd ds to clear the plan list and trigger emerg_rep*/
		bRet = killSecondDs();
		Assert.assertTrue(bRet);
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);	
		
		/* Check previous plan's priority */
		bRet = checkPreviousPlanIsEmergency(DEFAULT_CHECK_PLAN_NUM, DEFAULT_CHECK_PLAN_TIME_LIMIT);
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
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);
		
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
		
		/* Rotate ns log */
		bRet = rotateLog();
		Assert.assertTrue(bRet);

		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
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
		
		/* Start the 1st ds */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Check interrupt */
		bRet = checkInterrupt(2);
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
		
		bRet = tfsGrid.start();
		Assert.assertTrue(bRet);
		
		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	
	}
	
}
