/**
 * 
 */
package com.taobao.tfstest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator
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
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* sleep */
		sleep(30);
		
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
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* Monitor the read process */
		bRet = readCmdMon();
		Assert.assertTrue(bRet);
		
		/* Check rate */
		bRet = checkRateEnd(FAILRATE, READ );
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===> end");
		return ;
	}
	
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
		
		/* Wait 10s for recover */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Start */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 3);
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
		
		/* Wait 10s for recover */
		sleep (10);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY);
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Start */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 3);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 3);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 3);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDsOneSide();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 0);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
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
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
		Assert.assertTrue(bRet);
		bRet = chkBlockCnt(BLOCKCHKTIME, 2);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* Check the rate of write process */
		bRet = checkRateRun(FAILRATE, WRITEONLY|READ|UNLINK);
		Assert.assertTrue(bRet);
		
		/* All ds one side in */
		bRet = startAllDs();
		Assert.assertTrue(bRet);
		
		/* Check block copys */
		bRet = chkBlockCnt(BLOCKCHKTIME, 1);
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
		
		bRet = tfsGrid.start();
		Assert.assertTrue(bRet);
		
		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	
	}
}
