package com.taobao.common.tfs.testcase.function.rc;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class LoginOperationTest extends rcTfsBaseCase {
	@Test
	public void testInitWithClientRestart() {
		log.info("begin: " + getCurrentFunctionName());

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("@@ sessionId: " + sessionId);
		Assert.assertTrue(tfsStatus.checkSessionId(sessionId));
		tfsManager.destroy();

		tfsManager = createTfsManager();
		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId2 = tfsManager.getSessionId();
		Assert.assertFalse(sessionId.equals(sessionId2));
		Assert.assertTrue(tfsStatus.checkSessionId(sessionId2));
		tfsManager.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testInitSeveralClientsWithSameKey() {
		log.info("begin: " + getCurrentFunctionName());

		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager1 = createTfsManager();
		DefaultTfsManager tfsManager2 = createTfsManager();

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager1.getSessionId();
		log.debug("@@ sessionId: " + sessionId);
		Assert.assertTrue(tfsStatus.checkSessionId(sessionId));

		String sessionId2 = tfsManager2.getSessionId();
		Assert.assertFalse(sessionId.equals(sessionId2));
		Assert.assertTrue(tfsStatus.checkSessionId(sessionId2));
		tfsManager1.destroy();
		tfsManager2.destroy();

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testInitOneClientWithSameKey() {
		log.info("begin: " + getCurrentFunctionName());

		DefaultTfsManager tfsManager = createTfsManager();

		String sessionId = tfsManager.getSessionId();
		log.debug("@@ sessionId: " + sessionId);

		tfsManager.setRcAddr(tfsManager.getRcAddr());
		tfsManager.setAppKey(tfsManager.getAppKey());
		tfsManager.setAppIp(tfsManager.getAppIp());
		boolean result = tfsManager.init(); // with log info: rc manager is
											// already inited
		Assert.assertTrue(result);

		String sessionId2 = tfsManager.getSessionId();
		Assert.assertTrue(sessionId.equals(sessionId2));

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testInitOneClientWithDifferentKey() {
		log.info("begin: " + getCurrentFunctionName());

		DefaultTfsManager tfsManager = createTfsManager();

		String sessionId = tfsManager.getSessionId();
		log.debug("@@ sessionId: " + sessionId);

		String differentKey = "tappkey00002";
		tfsManager.setRcAddr(tfsManager.getRcAddr());
		tfsManager.setAppKey(differentKey);
		tfsManager.setAppIp(tfsManager.getAppIp());
		boolean result = tfsManager.init(); // with log info: rc manager is
											// already inited
		Assert.assertTrue(result);

		String sessionId2 = tfsManager.getSessionId();
		Assert.assertTrue(sessionId.equals(sessionId2));

		log.info("end: " + getCurrentFunctionName());
	}
}
