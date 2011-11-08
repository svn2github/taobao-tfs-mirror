package com.taobao.common.tfs.disastertest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.KillTypeEnum;

public class FailOver_name_meta_ha_test extends NameMetaBaseCase {
//	@Test
	public void FailOver_01_rs_ha_switch_once(){
		boolean bRet = false;
		caseName = "FailOver_01_rs_ha_switch_once";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmdStart();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(10);
		
        /*Start ls dir cmd*/
        bRet = isCreateLog();
        Assert.assertTrue(bRet);
      
        /*Ls dir moniter*/
        bRet = isCreateLogMin();
        Assert.assertTrue(bRet);
      
        /*Check the rate of write process */
        bRet = chkFailRate();
        Assert.assertTrue(bRet);

		/*Kill master rootserver*/
		bRet = killMasterRs();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(10);
		
		/*Start slave rootserver*/
		bRet = startSlaveRs();
		Assert.assertTrue(bRet);
				
		/*Wait for 10s*/
		sleep(10);
		

        /*Start ls dir cmd*/
        bRet = isCreateLog();
        Assert.assertTrue(bRet);

        /*Ls dir moniter*/
        bRet = isCreateLogMin();
        Assert.assertTrue(bRet);
       
        /*Check the rate of write process */
        bRet = chkFailRate();
        Assert.assertTrue(bRet);

		/*Stop create dir cmd*/
		bRet = createDirCmdEnd();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
//	@Test
	public void FailOver_02_rs_ha_switch_serval_times(){
		boolean bRet = false;
		caseName = "FailOver_02_rs_ha_switch_serval_times";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmdStart();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(10);
		
        /*Start ls dir cmd*/
        bRet = isCreateLog();
        Assert.assertTrue(bRet);

        /*Ls dir moniter*/
        bRet = isCreateLogMin();
        Assert.assertTrue(bRet);
       
        /*Check the rate of write process */
        bRet = chkFailRate();
        Assert.assertTrue(bRet);

		for (int iLoop = 0; iLoop < 4 ; iLoop++)
		{
			log.info("<-----case 2: kill master rootserver " + (iLoop+1) +" start----->");			

			/*Kill master rootserver*/
			bRet = killMasterRs();
			Assert.assertTrue(bRet);
			
			/*Wait for 10s*/
			sleep(20);
			
			/*Start slave rootserver*/
			bRet = startSlaveRs();
			Assert.assertTrue(bRet);

			/*Wait for 10s*/
			sleep(10);
	
			/*Start ls dir cmd*/
            bRet = isCreateLog();
	        Assert.assertTrue(bRet);

            /*Ls dir moniter*/
            bRet = isCreateLogMin();
            Assert.assertTrue(bRet);

       		/*Check the rate of write process */
	        bRet = chkFailRate();
            Assert.assertTrue(bRet);

			log.info("<------case 2: kill master rootserver " + (iLoop+1) +" end------>");

		}
		
//                /*Start ls dir cmd*/
 //               bRet = isCreateLog();
 //               Assert.assertTrue(bRet);
//
 //               /*Ls dir moniter*/
 //               bRet = isCreateLogMin();
 //               Assert.assertTrue(bRet);
 //              
 //               /*Check the rate of write process */
  //              bRet = chkFailRate();
  //              Assert.assertTrue(bRet);

		/*Stop create dir cmd*/
		bRet = createDirCmdEnd();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
	@Test
	public void FailOver_03_rs_ha_meta_in_switch_once(){
		boolean bRet = false;
		caseName = "FailOver_03_rs_ha_meta_in_switch_once";
		log.info(caseName + "===> start");
		
		/*Start create dir cmd*/
		bRet = createDirCmdStart();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(5);		

        /*Start ls dir cmd*/
        bRet = isCreateLog();
        Assert.assertTrue(bRet);

        /*Ls dir moniter*/
        bRet = isCreateLogMin();
        Assert.assertTrue(bRet);
       
        /*Check the rate of write process */
        bRet = chkFailRate();
        Assert.assertTrue(bRet);

		/*Kill metaserver*/
		bRet = killOneMetaserver(1);
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(5);

//                /*Start ls dir cmd*/
//                bRet = isCreateLog();
//                Assert.assertTrue(bRet);
//                
//                /*Ls dir moniter*/
//                bRet = isCreateLogMin();
//                Assert.assertTrue(bRet);
//
//                /*Check the rate of write process */
//                bRet = chkFailRate();
//                Assert.assertTrue(bRet);

		/*Kill master rootserver*/
		bRet = killMasterRs();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(10);
		
		/*Start metaserver*/
		bRet = startOneMetaserver(1);
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(10);
		
		/*Start slave rootserver*/
		bRet = startSlaveRs();
		Assert.assertTrue(bRet);
		
		/*Wait for 10s*/
		sleep(5);

        /*Start ls dir cmd*/
        bRet = isCreateLog();
        Assert.assertTrue(bRet);
        
        /*Ls dir moniter*/
        bRet = isCreateLogMin();
        Assert.assertTrue(bRet);

        /*Check the rate of write process */
        bRet = chkFailRate();
        Assert.assertTrue(bRet);
	
		/*Stop create dir cmd*/
		bRet = createDirCmdEnd();
		Assert.assertTrue(bRet);
		
		log.info(caseName + "===>end");
	}
//	@Test
	public void FailOver_04_rs_ha_meta_version(){
	    boolean bRet = false;
        caseName = "FailOver_04_rs_ha_meta_version";
        log.info(caseName + "===> start");
    
	    /*Start all root server*/
	    bRet = startAllRS();
	    Assert.assertTrue(bRet);

	    /*Wait for 10s*/
	    sleep(10);

	    /*Kill masterserver*/
	    bRet = killMasterRs();
        Assert.assertTrue(bRet);

        /*Wait for 10s*/
        sleep(10); 
   
        /*Start slave rootserver*/
        bRet = startSlaveRs();
        Assert.assertTrue(bRet);	
	    
	    /*Wait for 10s*/
	    sleep(10);
	    /*Clean up all HA*/
	    bRet = cleanUpAllHA();
	    Assert.assertTrue(bRet);

	    /*Wati for 5s*/
	    sleep(10);

	    /*Kill masterserver*/	
        bRet = killMasterRs();
        Assert.assertTrue(bRet);
                    
        /*Wait for 10s*/
        sleep(10);

        /*Clean up all HA*/
	    bRet = cleanUpAllHA();
	    Assert.assertTrue(bRet);

        /*Start slave rootserver*/
        bRet = startSlaveRs();
        Assert.assertTrue(bRet); 

        /*Wait for 10s*/
        sleep(10);

	    /*Compare rootserver version*/

	    log.info(caseName + "===>end");
	}		
	@After
	public void tearDown()
	{
	    /* Clean case name */
	    caseName = "";
		
	    /*Stop createDir*/
	    boolean bRet = false;
	    bRet = createDirCmdEnd();
	    Assert.assertTrue(bRet);
	}
	
	@Before
	public void setUp()
	{
	    /* Reset case name */
	    caseName = "";

	    /* Set failcount */
//	    boolean bRet = false;	
//	    bRet = resetAllFailCnt(MASTER_RS, SLAVE_RS);
//	    Assert.assertTrue(bRet);
	}

}
