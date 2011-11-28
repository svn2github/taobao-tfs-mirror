package com.taobao.common.tfs.disastertest;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.NameMetaBaseCase;
import com.taobao.gaia.KillTypeEnum;

public class FailOver_name_meta_ha_test extends NameMetaBaseCase {
	@Test
	public void FailOver_01_rs_ha_switch_once(){
		boolean bRet = false;
		caseName = "FailOver_01_rs_ha_switch_once";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmd(10);
		Assert.assertTrue(bRet);
		
		/*Wait for 5s*/
		sleep(5);
               
		/*Check the rate of write process */
                bRet = chkRateEnd(100,OPER_CREATE_DIR,10);
                Assert.assertTrue(bRet);

		/*Kill master rootserver*/
		bRet = killMasterRs();
		Assert.assertTrue(bRet);
		
		/*Start slave rootserver*/
		bRet = startSlaveRs();
		Assert.assertTrue(bRet);
				
		/*Wait for 5s*/
		sleep(5);
		
                /*Check the rate of write process */
                bRet = chkRateByRun(OPER_CREATE_DIR,10);
                Assert.assertTrue(bRet);

		/*Stop create dir cmd*/
		bRet = createDirCmdStop();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
	@Test
	public void FailOver_02_rs_ha_switch_serval_times(){
		boolean bRet = false;
		caseName = "FailOver_02_rs_ha_switch_serval_times";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmd(11);
		Assert.assertTrue(bRet);
		
		/*Wait for 3s*/
		sleep(10);

                /*Check the rate of write process */
                bRet = chkRateEnd(100,OPER_CREATE_DIR,11);
                Assert.assertTrue(bRet);

		for (int iLoop = 0; iLoop < 10 ; iLoop++)
		{
			/*Kill master rootserver*/
			bRet = killMasterRs();
			Assert.assertTrue(bRet);
			
			/*Start slave rootserver*/
			bRet = startSlaveRs();
			Assert.assertTrue(bRet);

			/*Wait for 3s*/
			sleep(3);
		}
		
		
                /*Wait for 2s*/
                sleep(2);

                /*Check the rate of write process */
                bRet = chkRateByRun(OPER_CREATE_DIR,11);
                Assert.assertTrue(bRet);

		/*Stop create dir cmd*/
		bRet = createDirCmdStop();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
	@Test
	public void FailOver_03_rs_ha_meta_in_switch_once(){
		boolean bRet = false;
		caseName = "FailOver_03_rs_ha_meta_in_switch_once";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmd(12);
		Assert.assertTrue(bRet);
		
		/*Wait for 5s*/
		sleep(10);		
	
	
                /*Check the rate of write process */
                bRet = chkRateEnd(100,OPER_CREATE_DIR,12);
                Assert.assertTrue(bRet);

		/*Kill metaserver*/
		bRet = killOneMetaserver(1);
		Assert.assertTrue(bRet);
		
		/*Wait for 5s*/
		sleep(5);
		

		/*Kill master rootserver*/
		bRet = killMasterRs();
		Assert.assertTrue(bRet);
		
		/*Start metaserver*/
		bRet = startOneMetaserver(1);
		Assert.assertTrue(bRet);
		
		/*Start slave rootserver*/
		bRet = startSlaveRs();
		Assert.assertTrue(bRet);
		
		/*Wait for 5s*/
		sleep(5);
		
                /*Check the rate of write process */
                bRet = chkRateByRun(OPER_CREATE_DIR,12);
                Assert.assertTrue(bRet);
	
		/*Stop create dir cmd*/
		bRet = createDirCmdStop();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
	
	@After
	public void tearDown()
	{
	    /* Clean case name */
	    caseName = "";
		       
	    /*Stop createDir*/
	    boolean bRet = false;
	    bRet = createDirCmdStop();
	    Assert.assertTrue(bRet);
	}
	
	@Before
	public void setUp()
	{
	    /* Reset case name */
	    caseName = "";

	    /* reset HA */
	    boolean bRet = false;	
	    bRet = resetAllFailCount();
	    Assert.assertTrue(bRet);
	}
}
