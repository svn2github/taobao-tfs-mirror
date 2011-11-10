package com.taobao.tfstest;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.taobao.gaia.HelpBase;

public class Function_Report_On_Time extends FailOverBaseCase {

	public HelpBase helpBase = new HelpBase();

	@Test
	public void ns_report_ontime_under_writepress() {
		boolean bRet = false;
		caseName = "ns_report_ontime_safemodetimeEqualzero_with_writepress";
		log.info("ns_report_ontime_safemodetimeEqualzero_with_writepress"
				+ "===> start");

		/* Write file */
		bRet = writeCmd();
		assertTrue(bRet);
		
		String serverIP = "10.232.4.1";
		String blkIp = "10.232.4.11";
		
		/*Block net*/
		helpBase.netBlockBase(serverIP, blkIp, 1, 5);
		
		/*wait 5s*/
		sleep(1000*5);
		
		/* Stop write file */
		bRet = writeCmdStop();
		assertTrue(bRet);
		helpBase.netUnblockBase(serverIP);
		
		 /* verification */
	}

	@Test
	   public void ns_report_safe_mode_time_equal_zero() {
	      boolean bRet = false;
	      caseName = " ns_report_safe_mode_time_equal_zero";
	      log.info(" ns_report_safe_mode_time_equal_zero"
	            + "===> start");
	      String serverIP = "10.232.4.1";
	      
	      /*set safe_mode_time=0*/
	      helpBase.confDelLineBase(serverIP,"tfsConf","67");
	      helpBase.confAddLineBase(serverIP, "tfsConf",67, "safe_mode_time =0");
	   
	      /*Write file*/
	      bRet = writeCmd();
	      assertTrue(bRet);
	      
	      /*wait 5s*/
	      sleep(1000*5);
	      
	      /* Stop write file */
	      bRet = writeCmdStop();
	      assertTrue(bRet);
	      
	      /* verification */

	      
	   }
	   
	
	

}
