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
import com.taobao.tfstest.FailOverBaseCase.PlanType;

/**
 * @author Administrator/mingyan
 *
 */
public class Function_ns_test extends FailOverBaseCase {
	
	@Test
	public void Function_01_happy_path(){
		
		boolean bRet = false;
		caseName = "Function_01_happy_path";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* sleep */
		sleep(20);
		
		/* Stop write process */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY);
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
		
		/* Unlink file */
		bRet = unlinkCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the unlink process */
		bRet = unlinkCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		bRet = chkFinalRetFail();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_02_one_ds_out_copy_block(){
		
		boolean bRet = false;
		caseName = "Function_02_one_ds_out_copy_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_EMERG_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Check multi replicated block */
		bRet = chkMultiReplicatedBlock();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_03_all_ds_out_one_side(){
		
		boolean bRet = false;
		caseName = "Function_03_all_ds_out_one_side";
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
		
		/* Kill one ds */
		bRet = killAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		//bRet = chkBlockCntBothNormal(BLOCKCOPYCNT-1);
		//Assert.assertTrue(bRet);
		
		/* Start */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_04_all_ds_out(){
		
		boolean bRet = false;
		caseName = "Function_04_all_ds_out";
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
		
		/* Kill one ds */
		bRet = killAllDs();
		Assert.assertTrue(bRet);
		
		/* Wait 20s for recover */
		sleep (20);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(0);
		Assert.assertTrue(bRet);
		
		/* Start */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_05_one_ds_in(){
		
		boolean bRet = false;
		caseName = "Function_05_one_ds_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* One ds in */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_06_all_ds_one_side_in(){
		
		boolean bRet = false;
		caseName = "Function_06_all_ds_one_side_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		//bRet = chkBlockCntBothNormal(BLOCKCOPYCNT-1);
		//Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_07_all_ds_in(){
		
		boolean bRet = false;
		caseName = "Function_07_all_ds_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = killAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_08_one_clean_ds_in(){
		
		boolean bRet = false;
		caseName = "Function_08_one_clean_ds_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Clean one ds */
		bRet = cleanOneDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* One ds in */
		bRet = startOneDs();
		Assert.assertTrue(bRet);
		
		/* Wait for move */
		sleep(100);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_09_all_clean_ds_one_side_in(){
		
		boolean bRet = false;
		caseName = "Function_09_all_clean_ds_one_side_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = cleanAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		//bRet = chkBlockCntBothNormal(BLOCKCOPYCNT-1);
		//Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_10_all_clean_ds_in(){
		
		boolean bRet = false;
		caseName = "Function_10_all_clean_ds_in";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Kill one ds */
		bRet = cleanAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(0);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_11_compact(){
		
		boolean bRet = false;
		caseName = "Function_11_compact";
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
		
		/* Start write cmd */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Wait for compact */
		sleep(300);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the final rate */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_13_rep_block(){
		
		boolean bRet = false;
		caseName = "Function_13_rep_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Modify MinReplication */
		bRet = setMinReplication(1);
		Assert.assertTrue(bRet);
		
		/* Modify MaxReplication */
		bRet = setMaxReplication(1);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(1);
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
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);

		/* Check dump plan log */
		bRet = checkPlan(PlanType.PLAN_TYPE_REPLICATE, BLOCK_CHK_TIME);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_14_move_block(){
		
		boolean bRet = false;
		caseName = "Function_14_move_block";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(100);
		Assert.assertTrue(bRet);
		
		/* Modify balance_max_diff_block_num */
		bRet = setBalanceMaxDiffBlockNum(1);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
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
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@Test
	public void Function_15_compact_block(){
		
		boolean bRet = false;
		caseName = "Function_15_compact_block";
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
	
	@Test
	public void Function_16_delete_block(){
		
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
		bRet = setUnlinkRatio(30);
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
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Kill the 1st ds */
		bRet = killOneDs();
		Assert.assertTrue(bRet);

		/* Wait 10s for ssm to update the latest info */
		sleep (10);
		
		/* Wait for completion of replication */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
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
	
	@Test
	public void Function_17_one_ds_out_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_17_one_ds_out_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
	
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
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
	
	@Test
	public void Function_18_one_ds_join_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_18_one_ds_join_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
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
	
	@Test
	public void Function_19_one_ds_restart_clear_plan(){
		
		boolean bRet = false;
		caseName = "Function_19_one_ds_restart_clear_plan";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
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

	@Test
	public void Function_20_new_elect_writable_block_algorithm(){
		
		boolean bRet = false;
		caseName = "Function_20_new_elect_writable_block_algorithm";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(100);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
		Assert.assertTrue(bRet);

		/* Wait */
		sleep(60);	
		
		/* Get the block num hold by each ds before write */
		HashMap<String, Integer> blockDisBefore = new HashMap<String, Integer>();
		bRet = getBlockDistribution(blockDisBefore);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Start sar to account network traffic */
		bRet = networkTrafMonStart(SAMPLE_INTERVAL, TEST_TIME/SAMPLE_INTERVAL);
		Assert.assertTrue(bRet);
	
		/* Wait */
		sleep(TEST_TIME);	
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY);
		Assert.assertTrue(bRet);

		/* Wait for completion of replication */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);

		/* Check network traffic balance */
		bRet = chkNetworkTrafBalance("eth0", RXBYTPERSEC_SD_COL);
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
	
	@Test
	public void Function_21_check_read_network_traffic_balance(){
		
		boolean bRet = false;
		caseName = "Function_21_check_read_network_traffic_balance";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setReadFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Start sar to account network traffic */
		bRet = networkTrafMonStart(SAMPLE_INTERVAL, TEST_TIME/SAMPLE_INTERVAL);
		Assert.assertTrue(bRet);
		
		/* Wait */
		sleep(TEST_TIME);	
		
		/* Stop cmd */
		bRet = readCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateEnd(SUCCESSRATE, READ);
		Assert.assertTrue(bRet);

		/* Check network traffic balance */
		bRet = chkNetworkTrafBalance("eth0", TXBYTPERSEC_SD_COL);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	@Test
	public void Function_22_modify_gc_wait_time(){
		
		boolean bRet = false;
		caseName = "Function_22_modify_gc_wait_time";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
		
		/* Set unlink ratio */
		bRet = setUnlinkRatio(30);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(10);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(7200);
		Assert.assertTrue(bRet);

		/* Check the rate of write process */
		bRet = checkRateRun(SUCCESSRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);

		/* Modify object_dead_max_time */
		bRet = setObjectDeadMaxTime(3600);
		Assert.assertTrue(bRet);
		
		/* Stop write cmd */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = chkFinalRetSuc();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}	
	
	@Test
	public void Function_23_write_mass_blocks(){
		
		boolean bRet = false;
		caseName = "Function_23_write_mass_blocks";
		log.info(caseName + "===> start");
		
		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);
		
		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);
	
		/* Get current used cap(before write) */
		HashMap<String, Double> usedCap = new HashMap<String, Double>();
		bRet = getUsedCap(usedCap);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);

		double chkValue = usedCap.get("Before") + CHK_WRITE_AMOUNT;
		bRet = chkUsedCap(chkValue);
		Assert.assertTrue(bRet);
		
		/* Stop write process */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(SUCCESSRATE, WRITEONLY);
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
	@After
	public void tearDown(){
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
