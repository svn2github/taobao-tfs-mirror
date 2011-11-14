package com.taobao.common.tfs.function;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Date;
import com.taobao.common.tfs.function.RcBaseCase;
import com.taobao.common.tfs.DefaultTfsManager;

public class mergeRcMetaManager_02_meta_unlinkFile_operation extends RcBaseCase {
	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}

	@Test
	public void Function_01_rmFile_1k() {
		caseName = "Function_01_rmFile_1k";
		log.info(caseName + "===> start");

		boolean bRet = false;
		String localFile = "1k.jpg";
		String filePath = "/rmFile_1k" + d.format(new Date());
		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);

		/* Create DefaultTfsManager instance, and saveFile */
		tfsManager = CreateTfsManager();
		appId = tfsManager.getAppId();
		bRet = tfsManager.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(bRet);

		sleep(MAX_UPDATE_TIME);
		String sessionId = tfsManager.getSessionId();
		Assert.assertTrue(getFileSize(sessionId, 2) == 1024);

		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);

		bRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertTrue(bRet);
		sleep(MAX_UPDATE_TIME);

		Assert.assertTrue(getFileSize(sessionId, 4) == 1024);
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity, newUsedCapacity);

		bRet = tfsManager.fetchFile(appId, userId, "TEMP", "/test_1k_rm");
		Assert.assertFalse(bRet);

		tfsManager.destroy();
		log.info(caseName + "==> end");
	}

	@Test
	public void Function_02_rmFile_2M() {
		caseName = "Function_02_rmFile_2M";
		log.info(caseName + "===> start");

		boolean bRet = false;
		String localFile = "2M.jpg";
		String filePath = "/rmFile_2M" + d.format(new Date());
		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);

		tfsManager = CreateTfsManager();
		appId = tfsManager.getAppId();
		bRet = tfsManager.saveFile(appId, userId, localFile, filePath);
		assertTrue(bRet);

		sleep(MAX_UPDATE_TIME);
		String sessionId = tfsManager.getSessionId();
		Assert.assertTrue(getFileSize(sessionId, 2) == 2 * 1024 * 1024);

		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 2 * 1024 * 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);

		bRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertTrue(bRet);
		sleep(MAX_UPDATE_TIME);

		sessionId = tfsManager.getSessionId();
		Assert.assertTrue(getFileSize(sessionId, 4) == 2 * 1024 * 1024);

		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		assertEquals(oldUsedCapacity, newUsedCapacity);
		assertEquals(oldFileCount, newFileCount);

		bRet = tfsManager.fetchFile(appId, userId, retLocalFile, filePath);
		Assert.assertFalse(bRet);
		tfsManager.destroy();

		log.info(caseName + "===> end");
	}

	@Test
	public void Function_03_rmFile_3M() {

		caseName = "Function_03_rmFile_3M";
		log.info(caseName + "===> start");

		boolean bRet = false;
		String localFile = "3M.jpg";
		String filePath = "/rmFile_3M" + d.format(new Date());
		List<String> rootServerAddrList = new ArrayList<String>();
		rootServerAddrList.add(rsAddr);
		tfsManager = new DefaultTfsManager();
		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		tfsManager.setUseNameMeta(true);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);

		bRet = tfsManager.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(bRet);

		sleep(MAX_STAT_TIME * 2);
		String sessionId = tfsManager.getSessionId();
		// Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
		Assert.assertTrue(getFileSize(sessionId, 2) == 3 * 1024 * 1024);
		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 3 * 1024 * 1024, newUsedCapacity);
		// Assert.assertEquals(oldFileCount + 1, newFileCount);
		tfsManager.destroy();

		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		tfsManager.setUseNameMeta(true);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);

		bRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertTrue(bRet);

		sleep(MAX_STAT_TIME);
		sessionId = tfsManager.getSessionId();
		// Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
		Assert.assertTrue(getFileSize(sessionId, 4) == 3 * 1024 * 1024);
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
		// Assert.assertEquals(oldFileCount, newFileCount);

		bRet = tfsManager.fetchFile(appId, userId, retLocalFile, filePath);
		Assert.assertFalse(bRet);

		sleep(MAX_STAT_TIME);
		// Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
		// Assert.assertTrue(getFileSize(sessionId, 1) == 0);
		tfsManager.destroy();

		log.info(caseName + "===> end");
	}

	@Test
	public void Function_04_rmFile_6G() {
		caseName = "Function_04_rmFile_6G";
		log.info(caseName + "===> start");

		boolean bRet = false;
		String localFile = "6G.jpg";
		String filePath = "/rmFile_6G" + d.format(new Date());
		long expectSize = (long) (6 * ((long) 1 << 30));
		List<String> rootServerAddrList = new ArrayList<String>();
		rootServerAddrList.add(rsAddr);

		tfsManager = new DefaultTfsManager();
		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		tfsManager.setUseNameMeta(true);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		log.debug(" oldUsedCapacity: " + oldUsedCapacity + " oldFileCount: "
				+ oldFileCount);

		bRet = tfsManager.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(bRet);

		sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		// Assert.assertTrue(getOperTimes(sessionId, 2) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 2) == 1);
		Assert.assertEquals(expectSize, getFileSize(sessionId, 2));
		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + expectSize, newUsedCapacity);
		// Assert.assertEquals(oldFileCount + 1, newFileCount);
		tfsManager.destroy();

		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		tfsManager.setUseNameMeta(true);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		bRet = tfsManager.rmFile(appId, userId, filePath);
		Assert.assertTrue(bRet);

		sleep(MAX_STAT_TIME);
		sessionId = tfsManager.getSessionId();
		// Assert.assertTrue(getOperTimes(sessionId, 4) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 4) == 1);
		Assert.assertEquals(expectSize, getFileSize(sessionId, 4));
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);

		Assert.assertEquals(oldUsedCapacity, newUsedCapacity);
		// Assert.assertEquals(oldFileCount, newFileCount);
		bRet = tfsManager.fetchFile(appId, userId, "TEMP", filePath);
		Assert.assertFalse(bRet);
		// Assert.assertTrue(bRet);

		// sleep(MAX_STAT_TIME);
		// Assert.assertTrue(getOperTimes(sessionId, 1) == 1);
		// Assert.assertTrue(getSuccTimes(sessionId, 1) == 0);
		// Assert.assertTrue(getFileSize(sessionId, 1) == 0);
		tfsManager.destroy();

		log.info(caseName + "===> end");
	}
}
