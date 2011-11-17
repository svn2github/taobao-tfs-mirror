package com.taobao.tfstest;

import static org.junit.Assert.*;
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

	/*
	[步骤]
	1.配置10个ds，其中9个ds在一台物理机，另1个ds单独在另一台物理机，
	ns配置1个汇报线程，1个汇报队列，副本数为2，
	ns配置max_write_file_count为10，配置block_max_size为10M，
	设置定时汇报时间间隔为5分钟
	2.限制ns与那台只有1个ds的物理机的网络带宽
	3.启动客户端测试工具指定blockId带创建block标志位打开文件，执行写操作，写单位为1M
	
	[验证]
	每隔5分钟，当轮到该ds汇报时可以看到写失败，汇报过后写成功
	[备注]
	*/
	@Test
	public void test_1()
	{
		
	}
	/*
	步骤
	[步骤]
	1.配置4个ds，ds挂载容量相同，副本数为2，配置max_write_filecount为10，配置block_max_size为10MB
	2.启动客户端测试工具执行写操作，写单位为1M，持续较长时间
	
	[验证]
	写完毕后检测各ds上新增的block数差不多
	[备注]
	*/
	@Test
	public void ds_situation_is_same_with_conditions()
	{
		int maxWriteFilecount = 10;
		boolean bRet = false;
		caseName = "ds_situation_is_same_with_conditions";
		log.info(caseName + "===> start");
		String targetIp = MASTERIP;


		/* set max_write_filecount and block_max_size */
		bRet = conf.confReplaceSingle(targetIp, NSCONF, "max_write_filecount", String.valueOf(maxWriteFilecount));
		assertTrue(bRet);

		/* both ds.conf and ns.conf exist block_max_size, which one will take effect */
		for(int i = DSINDEX; i <= DS_CLUSTER_NUM; i++)
		{
			for(int j = 0; j < tfsGrid.getCluster(i).getServerList().size(); j++)
			{
				String strDsConf = tfsGrid.getCluster(i).getServer(j).getDir() + 
							tfsGrid.getCluster(i).getServer(j).getConfname();
				targetIp = tfsGrid.getCluster(i).getServer(j).getIp();
				bRet = conf.confReplaceSingle(targetIp, strDsConf, "block_max_size", String.valueOf(maxWriteFilecount));
				assertTrue(bRet);
			}
		}

		/* set write file size to 1M */
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, "size", String.valueOf(1<<20));
		assertTrue(bRet);
	
		/* start grid */
		startOneGrid(tfsGrid);
		sleep(30);
		
		/* write for 500s */
		writeCmd();
		sleep(500);
		writeCmdStop();

		/* check block number */
		String strCmd;
		ArrayList<String> result = new ArrayList<String>();
		
		strCmd = TFS_HOME + "bin/ssm -s ";
		strCmd += MASTERIP + ":" + MASTERSER.getPort();
		strCmd += " -i 'machine -a'";
		bRet = Proc.cmdOutBase(CLIENTIP, strCmd, "[ 0-9]*", 5, null, result);
		assertTrue(bRet);
		
		for(int i = 0; i < result.size() - 1; i++)
		{
			assertEquals(Integer.parseInt(result.get(i))/10, Integer.parseInt(result.get(i+1))/10);
		}

	}
	/*
	步骤
	[步骤]
	1.配置4个ds，ds挂载容量相同，副本数为2，配置max_write_filecount为10，配置block_max_size为10MB
	2.启动客户端测试工具执行写操作，写单位为1M，持续较长时间
	
	[验证]
	写完毕后检测各ds上新增的block数差不多
	[备注]
	*/
	@Test
	public void test_3()
	{
		
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
