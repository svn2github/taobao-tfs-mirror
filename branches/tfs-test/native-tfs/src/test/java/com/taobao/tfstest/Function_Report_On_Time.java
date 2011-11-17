package com.taobao.tfstest;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.gaia.AppGrid;
import com.taobao.gaia.HelpBase;
import com.taobao.gaia.KillTypeEnum;

public class Function_Report_On_Time extends FailOverBaseCase {

	public HelpBase helpBase = new HelpBase();
	public static final String strOnTimeReportLog = "report block on time";

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

		/* Block net */
		helpBase.netBlockBase(serverIP, blkIp, 1, 5);

		/* wait 5s */
		sleep(1000 * 5);

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
		log.info(" ns_report_safe_mode_time_equal_zero" + "===> start");
		String serverIP = "10.232.4.1";

		/* set safe_mode_time=0 */
		helpBase.confDelLineBase(serverIP, "tfsConf", "67");
		helpBase.confAddLineBase(serverIP, "tfsConf", 67, "safe_mode_time =0");

		/* Write file */
		bRet = writeCmd();
		assertTrue(bRet);

		/* wait 5s */
		sleep(1000 * 5);

		/* Stop write file */
		bRet = writeCmdStop();
		assertTrue(bRet);

		/* verification */

	}

	/*
	 * 步骤：1.设置ns配置中的定时汇报时间间隔 2. 定时汇报工作正常
	 */
	@Test
	public void ontime_report_happy_path() {
		int safeModeTime = 10;
		caseName = "ns_switch_one_time_ontime";
		log.info(caseName + "===> start");
		String targetIp = MASTERIP;

		/* set safe_mode_time to 10 */
		conf.confReplaceSingle(targetIp, NSCONF, "safe_mode_time", String.valueOf(safeModeTime));
		startOneGrid(tfsGrid);

		/* wait one round of report */
		sleep(safeModeTime + 10);

		/* wait ns startup */
		sleep(10);

		/* verify - check the report log in nameserver.log */
		assertTrue(checkOnTimeReportLog(targetIp));
	}

	/*
	 * 步骤： 1.设置ns配置中的定时汇报时间间隔  2.等一轮汇报之后，ns发生切换  3.看切换之后的定时汇报情况
	 */
	@Test
	public void ns_switch_one_time_ontime() {
		int safeModeTime = 10;
		caseName = "ns_switch_one_time_ontime";
		log.info(caseName + "===> start");
		String targetIp = MASTERIP;

		/* set safe_mode_time to 10 */
		conf.confReplaceSingle(targetIp, NSCONF, "safe_mode_time", String.valueOf(safeModeTime));
		startOneGrid(tfsGrid);

		/* wait one round of report */
		sleep(safeModeTime + 10);
		
		killMasterNs();
		startSlaveNs();
		
		/* wait ns startup */
		sleep(10);

		/* verify - check the report log in nameserver.log */
		assertTrue(checkOnTimeReportLog(targetIp));
	}

	/*
	1.使用4个ds，ns配置1个汇报线程，汇报队列大小为1，定时汇报时间间隔设得较短（5分钟）
	2.将其中1台ds频繁重启若干次
	*/
	@Test
	public void ds_switch_some_time_ontime() {
		boolean bRet = false;
		int safeModeTime = 10;
		caseName = "ds_switch_some_time_ontime";
		log.info(caseName + "===> start");
		String targetIp = MASTERIP;

		/* set safe_mode_time to 10 */
		conf.confReplaceSingle(targetIp, NSCONF, "safe_mode_time", String.valueOf(safeModeTime));
		startOneGrid(tfsGrid);

		/* wait one round of report */
		sleep(safeModeTime + 10);

		for(int i = 0; i < 5; i++)
		{
			/* kill one ds */
			bRet = killOneDs();
			assertTrue(bRet);

			/* restart one ds */
			bRet = startOneDs();
			assertTrue(bRet);

			sleep(5);
		}

		/* verify - check the report log in nameserver.log */
		assertTrue(checkOnTimeReportLog(targetIp));
	}

	private void startOneGrid(AppGrid appGrid) {
		boolean bRet = false;
		/* Set the failcount */
		bRet = setAllFailCnt();
		Assert.assertTrue(bRet);

		/* Kill the grid */
		bRet = appGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		Assert.assertTrue(bRet);

		/* Set Vip */
		bRet = migrateVip();
		Assert.assertTrue(bRet);

		/* Clean the log file */
		bRet = appGrid.clean();
		Assert.assertTrue(bRet);

		bRet = appGrid.start();
		Assert.assertTrue(bRet);

		/* Set failcount */
		bRet = resetAllFailCnt();
		Assert.assertTrue(bRet);
	}

	private boolean checkOnTimeReportLog(String targetIp)
	{
		ArrayList<String> result = new ArrayList<String>(500);
		String strCmd = "cat " + TFS_HOME + "/logs/nameserver.log | grep " + strOnTimeReportLog;

		if(Proc.proStartBase(targetIp, strCmd, result) == false)
		{
			log.error("Failed to checkOnTimeReportLog!");
			return false;
		}

		if(result.size() > 0)
			return true;
		return false;
	}
	@Before
	public void setUp() {
		/* Reset case name */
		caseName = "";
	}
	@After
	public void tearDown()
	{
		boolean bRet;

		bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
		assertTrue(bRet);
	}
}
