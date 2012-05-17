package com.taobao.common.tfs.testcase.function.rc;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class ClusterPermissionTest extends BaseCase {
	private int clusterPerssion = 0;
	
	@Test
	public void testClusterInvalid(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean result = false;

		String tfsname = "";
		String localFile = "1B.jpg";

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, INVALID_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);
		long startTime = System.currentTimeMillis();
		while (tfsname != null) {
			tfsname = tfsManager.saveFile(localFile, null, null);
		}
		long endTime = System.currentTimeMillis();
		log.debug("change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			tfsname = tfsManager.saveFile(localFile, nameList.get(i), null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testClusterR(){
		log.info("begin: " + getCurrentFunctionName());

		boolean result = false;

		String tfsname = "";
		String localFile = "1B.jpg";

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, R_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);
		long startTime = System.currentTimeMillis();
		while (tfsname != null) {
			tfsname = tfsManager.saveFile(localFile, null, null);
		}
		long endTime = System.currentTimeMillis();
		log.debug("change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			tfsname = tfsManager.saveFile(localFile, nameList.get(i), null);
			Assert.assertNotNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertTrue(result);
		}

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test //something wrong with this test case
	public void testClusterRW(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean ret = false;
		String localFile = "1B.jpg";
		String newlocalFile = "2M.jpg";
		String tfsname = "";
		
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();

		tfsStatus.setClusterPermissionByAppKey(appKey, RW_MODE);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);

			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);

			tfsname = tfsManager.saveFile(newlocalFile, tfsname, null);
			log.debug("@@ update tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);

			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);

			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testChangeClusterModeFromRW2Invalid(){
		log.info("begin: " + getCurrentFunctionName());

		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ normal cluster tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);

			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(result);
		}
		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		// change permission
		tfsStatus.setClusterPermissionByAppKey(appKey, INVALID_MODE);

		// test time
		long startTime = System.currentTimeMillis();
		while (tfsname != null) {
			tfsname = tfsManager.saveFile(localFile, null, null);
		}
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testChangeClusterModeFromRW2R(){
		log.info("begin: " + getCurrentFunctionName());

		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		tfsStatus.setClusterPermissionByAppKey(appKey, RW_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);

			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(result);
		}

		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, R_MODE);

		// test time
		long startTime = System.currentTimeMillis();
		while (tfsname != null) {
			tfsname = tfsManager.saveFile(localFile, null, null);
		}
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertTrue(result);
		}

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testChangeClusterModeFromInvalid2R(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, INVALID_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, R_MODE);

		// test time
		int n = 0;
		long startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);
			result = tfsManager
					.fetchFile(nameList.get(n % 10), null, "localfile");
			n++;
		} while (result != true);
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertTrue(result);
		}
		
		log.info("end" + getCurrentFunctionName());
	}
	
	@Test
	public void testChangeClusterModeFromInvalid2RW(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, INVALID_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, RW_MODE);

		// test time
		int n = 0;
		long startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			result = tfsManager
					.fetchFile(nameList.get(n % 10), null, "localfile");
			n++;
		} while (tfsname == null && result == false);
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);

			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(result);
		}
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testChangeClusterModeFromR2Invalid(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";

		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, R_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertTrue(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertTrue(result);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, INVALID_MODE);

		// test time
		int n = 0;
		long startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			result = tfsManager
					.fetchFile(nameList.get(n % 10), null, "localfile");
			n++;
		} while (tfsname == null && result == true);
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	
	@Test
	public void testChangeClusterModeFromR2RW(){
		log.info("begin: " + getCurrentFunctionName());
		
		boolean ret = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();
		ArrayList<String> nameList = new ArrayList<String>();

		tfsStatus.setClusterPermissionByAppKey(appKey, RW_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		// file for fetch and unlink when invalid
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, R_MODE);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNull(tfsname);

			ret = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertTrue(ret);
		}

		tfsStatus.setClusterPermissionByAppKey(appKey, RW_MODE);

		// test time
		int n = 0;
		long startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			ret = tfsManager
					.fetchFile(nameList.get(n % 10), null, "localfile");
			n++;
		} while (tfsname == null && ret == true);
		long endTime = System.currentTimeMillis();
		log.debug("@@ change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		// new stat check
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			Assert.assertNotNull(tfsname);

			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);

			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}
		
		log.info("end: " + getCurrentFunctionName());
		
	}
	
	
	private void backPermission() {
		TfsStatus tfsStatus = new TfsStatus();
		clusterPerssion = tfsStatus.getClusterPermission(appKey);
	}

	private void restorePerssion() {
		TfsStatus tfsStatus = new TfsStatus();
		tfsStatus.setClusterPermissionByAppKey(appKey, clusterPerssion);
	}

	@Before
	public void setUp() {
		backPermission();
	}

	@After
	public void tearDown() {
		super.tearDown();
		restorePerssion();
		TimeUtility.sleep(MAX_UPDATE_TIME);
	}
	
	
}
