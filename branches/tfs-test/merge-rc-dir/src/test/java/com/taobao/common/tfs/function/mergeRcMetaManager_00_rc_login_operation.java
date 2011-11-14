/**
 * 
 */
package com.taobao.common.tfs.function;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;

/**
 * @author Administrator/chuyu
 * 
 */
public class mergeRcMetaManager_00_rc_login_operation extends RcBaseCase {

	@Before
	public void setUp() {

	}

	@After
	public void tearDown() {
		tfsManager.destroy();
	}

	public DefaultTfsManager CreateTfsManager()
	{
		DefaultTfsManager dtTfsManager;
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		dtTfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = dtTfsManager.getAppId();

		return dtTfsManager;
	}

	@Test
	public void Function_01_init_with_client_restart() {

		caseName = "Function_01_init_with_client_restart";
		log.info(caseName + "===> start");

		tfsManager = CreateTfsManager();
		String sessionId = tfsManager.getSessionId();
		log.debug("@@ sessionId: " + sessionId);
		Assert.assertTrue(checkSessionId(sessionId));
		tfsManager.destroy();

		tfsManager = CreateTfsManager();
		String sessionId2 = tfsManager.getSessionId();
		Assert.assertFalse(sessionId.equals(sessionId2));
		Assert.assertTrue(checkSessionId(sessionId2));
		tfsManager.destroy();

		log.info(caseName + "===> end");

	}

	@Test
	public void Function_02_init_several_clients_with_same_key() {

		caseName = "Function_02_init_several_clients_with_same_key";
		log.info(caseName + "===> start");

		tfsManager = CreateTfsManager();
		String sessionId = tfsManager.getSessionId();
		Assert.assertTrue(checkSessionId(sessionId));

		DefaultTfsManager tfsManager2 = CreateTfsManager();
		String sessionId2 = tfsManager2.getSessionId();
		Assert.assertTrue(checkSessionId(sessionId2));

		log.info("sessionId 1:" + sessionId);
		log.info("sessionId 2:" + sessionId2);
		Assert.assertFalse(sessionId.equals(sessionId2));

		tfsManager.destroy();
		tfsManager2.destroy();
		log.info(caseName + "===> end");
	}

	@Test
	public void Function_03_init_a_client_with_same_key() {

		caseName = "Function_03_init_a_client_with_same_key";
		log.info(caseName + "===> start");

		boolean bRet = false;
		tfsManager = CreateTfsManager();
		String sessionId = tfsManager.getSessionId();

		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();

		Assert.assertTrue(bRet);
		String sessionId2 = tfsManager.getSessionId();

		Assert.assertTrue(sessionId.equals(sessionId2));
		log.info(caseName + "===> end");
	}

	@Test
	public void Function_04_init_a_client_with_different_key() {

		caseName = "Function_04_init_a_client_with_different_key";
		log.info(caseName + "===> start");

		boolean bRet = false;

		tfsManager = CreateTfsManager();
		String sessionId = tfsManager.getSessionId();

		String appKey2 = "tappkey00003";
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey2);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet); 
		String sessionId2 = tfsManager.getSessionId();

		Assert.assertTrue(sessionId.equals(sessionId2));

		log.info(caseName + "===> end");
	}

}
