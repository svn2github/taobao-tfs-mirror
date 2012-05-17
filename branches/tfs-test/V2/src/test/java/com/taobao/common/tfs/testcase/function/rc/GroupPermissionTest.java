package com.taobao.common.tfs.testcase.function.rc;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class GroupPermissionTest extends BaseCase {
	private int groupPermission = 0;
	private int clusterPerssion = 0;
	
	@Test
	public void testGroupInvalidClusterRW(){
		log.info("begin: " + getCurrentFunctionName());
		
		
		testClusterGroupPermission(INVALID_MODE,RW_MODE);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testGroupInvalidClusterR(){
		log.info("begin: " + getCurrentFunctionName());
		
		testClusterGroupPermission(INVALID_MODE,R_MODE);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testGroupRClusterInvalid(){
		log.info("begin: " + getCurrentFunctionName());

		testClusterGroupPermission(R_MODE,INVALID_MODE);

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testGroupRClusterRW(){
		log.info("begin: " + getCurrentFunctionName());

		testClusterGroupPermission(R_MODE,RW_MODE);

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testGroupRWClusterInvalid(){
		log.info("begin: " + getCurrentFunctionName());

		testClusterGroupPermission(RW_MODE,INVALID_MODE);

		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testGroupRWClusterR(){
		log.info("begin: " + getCurrentFunctionName());

		testClusterGroupPermission(RW_MODE,R_MODE);

		log.info("end: " + getCurrentFunctionName());
	}
	
	
	private void testClusterGroupPermission(int clusterPermission,
			int groupPermission) {
		boolean result = false;
		String tfsname = "";
		String localfile = "10K.jpg";
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();
		ArrayList<String> nameList = new ArrayList<String>();

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localfile, null, null);
			Assert.assertNotNull(tfsname);
			log.debug("@@ tfsname: " + tfsname);
			nameList.add(tfsname);
		}

		tfsStatus.setGroupPermission(appKey, clusterPermission);
		tfsStatus.setClusterPermissionByAppKey(appKey, groupPermission);
		TimeUtility.sleep(MAX_UPDATE_TIME);

		long startTime = System.currentTimeMillis();
		while (tfsname != null) {
			tfsname = tfsManager.saveFile(localfile, null, null);
		}
		long endTime = System.currentTimeMillis();
		log.debug("change time: " + (endTime - startTime));
		Assert.assertTrue((endTime - startTime) / 1000 < MAX_UPDATE_TIME);

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localfile, null, null);
			Assert.assertNull(tfsname);

			result = tfsManager.fetchFile(nameList.get(i), null, "localfile");
			Assert.assertFalse(result);

			result = tfsManager.unlinkFile(nameList.get(i), null);
			Assert.assertFalse(result);
		}
	}
	
	private void backPermission(){
		TfsStatus tfsStatus = new TfsStatus();
		clusterPerssion = tfsStatus.getClusterPermission(appKey);
		groupPermission = tfsStatus.getGroupPermission(appKey);
	}
	
	private void restorePerssion(){
		TfsStatus tfsStatus = new TfsStatus();
		tfsStatus.setClusterPermissionByAppKey(appKey, clusterPerssion);
		tfsStatus.setGroupPermission(appKey, groupPermission);
	}
	
	@Before
	public void setUp(){
		backPermission();
	}
	
	@After
	public void tearDown(){
		super.tearDown();
		restorePerssion();
		TimeUtility.sleep(MAX_UPDATE_TIME);
	}
}
