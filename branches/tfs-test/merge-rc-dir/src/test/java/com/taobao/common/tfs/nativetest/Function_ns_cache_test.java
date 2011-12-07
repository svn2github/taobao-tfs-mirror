/**
 * 
 */
package com.taobao.common.tfs.function;

import java.util.ArrayList;
import java.util.HashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator/mingyan
 *
 */
public class Function_ns_cache_test extends NativeTfsBaseCase {
	
  public String remote_cache_server_ipa = "10.232.12.0/24";
  public String remote_cache_server_ipb = "10.232.15.0/24";

	@Test
	public void Function_01_netblock_client_with_remote_cache_server(){
		
		boolean bRet = false;
		caseName = "Function_01_netblock_client_with_remote_cache_server";
		log.info(caseName + "===> start");
		
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
		sleep(600);

		/* block client with remote cache servers */
		bRet = Proc.netFullBlockBase(CLIENTIP, remote_cache_server_ipa);
		Assert.assertTrue(bRet);		

		/* block client with remote cache servers */
		bRet = Proc.netFullBlockBase(CLIENTIP, remote_cache_server_ipb);
		Assert.assertTrue(bRet);		

		/* sleep */
		sleep(600);
		
		/* netUnblock */
		bRet = Proc.netUnblockBase(CLIENTIP);
		Assert.assertTrue(bRet);

			/* sleep */
		sleep(600);

		/* Stop write process */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);

		/* Check rate */
		//bRet = checkRateEnd(SUCCESSRATE, WRITEONLY);
		//Assert.assertTrue(bRet);
		
		/* Read file */
		bRet = readCmd();
		Assert.assertTrue(bRet);
		
		/* sleep */
		sleep(120);

		/* block client with remote cache servers */
		bRet = Proc.netFullBlockBase(CLIENTIP, remote_cache_server_ipa);
		Assert.assertTrue(bRet);		

		/* block client with remote cache servers */
		bRet = Proc.netFullBlockBase(CLIENTIP, remote_cache_server_ipb);
		Assert.assertTrue(bRet);		

		/* sleep */
		sleep(120);
		
		/* netUnblock */
		bRet = Proc.netUnblockBase(CLIENTIP);
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
