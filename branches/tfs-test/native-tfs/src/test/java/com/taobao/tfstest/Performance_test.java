package com.taobao.tfstest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator/mingyan
 *
 */
public class Performance_test extends FailOverBaseCase {

	@Test
	public void Function_01_perf_happy_path(){
		
		boolean bRet = false;
		caseName = "Function_01_perf_happy_path";
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
		
		/* Check block copys */
		bRet = chkBlockCntBothNormal(BLOCKCOPYCNT);
		Assert.assertTrue(bRet);
		
		/* Write file */
		bRet = writeCmd();
		Assert.assertTrue(bRet);
		
		/* sleep */
		sleep(TEST_TIME);
		
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
		//bRet = mvSeedFile();
		//Assert.assertTrue(bRet);
		
		/* Clean the caseName */
		caseName = "";
	}
	
	@Before
	public void setUp(){
		boolean bRet = false;
		
		/* Reset case name */
		caseName = "";
	
		if (!grid_started)
		{
			/* Set the failcount */
			bRet = setAllFailCnt();
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
			
			grid_started = true;
		}
	
	}
}
