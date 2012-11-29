package com.taobao.common.tfs.testcase.function.rc;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.TimeUtility;

public class RcServerChangeTest extends rcTfsBaseCase {
	private String origRc = "";
	private String newRc = "";
	private List<String> addedRcs = new ArrayList<String>();

	@Test
	public void testModifyValidRc() {

		log.info("begin: " + getCurrentFunctionName());

		String origRc = tfsManager.getRcAddr();
		String newRc = "10.232.4.8:7269";
		this.origRc = origRc;
		this.newRc = newRc;

		testModifyRc(origRc, newRc);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testModifyInValidRc() {
		log.info("begin: " + getCurrentFunctionName());

		String origRc = tfsManager.getRcAddr();
		String newRc = tfsManager.getRcAddr() + "1";
		this.origRc = origRc;
		this.newRc = newRc;

		testModifyRc(origRc, newRc);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testChangeAllRcMode() {
		log.info("begin: " + getCurrentFunctionName());

		boolean ret = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		TfsStatus tfsStatus = new TfsStatus();

		long startTime = 0;
		long endTime = 0;

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}

		String validRc = "10.232.4.8:7269";
		addedRcs.add(validRc);
		tfsStatus.addRc(validRc, RC_VALID_MODE);
		tfsStatus.setRcMode(RC_INVALID_MODE);

		// check log to test the result, all rc will do nothing
		log.debug("@@ modify rc status begin -->");
		startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
			endTime = System.currentTimeMillis();
			log.debug("starttime: " + startTime + "endTime: " + endTime
					+ "change time: " + (endTime - startTime) / 1000);
		} while ((endTime - startTime) / 1000 < MAX_UPDATE_TIME);
		log.debug("@@ modify rc status end -->");

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testChangeRcServerMode() {
		log.info("begin: " + getCurrentFunctionName());

		boolean ret = false;
		String tfsname = "";
		String localFile = "1b.jpg";
		TfsStatus tfsStatus = new TfsStatus();

		long startTime = 0;
		long endTime = 0;

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}

		tfsStatus.setRcMode(tfsManager.getRcAddr(), RC_INVALID_MODE);

		// check log to test the result, rc will change to another
		log.debug("@@ modify rc status begin -->");
		long oldCapacity = tfsStatus.getUsedCapacity(appKey);
		long count = 0;
		startTime = System.currentTimeMillis();
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
			endTime = System.currentTimeMillis();
			log.debug("starttime: " + startTime + "endTime: " + endTime
					+ "change time: " + (endTime - startTime) / 1000);
			count++;
		} while ((endTime - startTime) / 1000 < MAX_UPDATE_TIME);
		log.debug("@@ modify rc status end -->");
		TimeUtility.sleep(MAX_STAT_TIME);
		long newCapacity = tfsStatus.getUsedCapacity(appKey);
		log.debug("@@ count: " + count + ", newCapacity: " + newCapacity
				+ ", oldCapacity: " + oldCapacity);
		Assert.assertTrue(count > (newCapacity - oldCapacity));

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			ret = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(ret);
			ret = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(ret);
		}

		log.info("end: " + getCurrentFunctionName());
	}

	// @Test
	public void testAddRcServer() {
		log.info("begin: " + getCurrentFunctionName());

		log.info("end: " + getCurrentFunctionName());
	}

	protected void testModifyRc(String origRc, String newRc) {
		TfsStatus tfsStatus = new TfsStatus();

		boolean result = false;
		String tfsname = "";
		String localFile = "1B.jpg";
		long startTime, endTime;

		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(result);
		}

		tfsStatus.modifyRc(origRc, newRc);

		log.debug("@@ modify rc status begin -->");
		long count = 0;
		startTime = System.currentTimeMillis();
		long oldCapacity = tfsStatus.getUsedCapacity(appKey);
		do {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);
			endTime = System.currentTimeMillis();
			log.debug("starttime: " + startTime + "endTime: " + endTime
					+ "change time: " + (endTime - startTime) / 1000);
			count++;
		} while ((endTime - startTime) / 1000 < MAX_UPDATE_TIME);
		log.debug("@@ modify rc status end -->");
		TimeUtility.sleep(MAX_STAT_TIME);
		long newCapacity = tfsStatus.getUsedCapacity(appKey);
		log.debug("@@ count: " + count + ", newCapacity: " + newCapacity
				+ ", oldCapacity: " + oldCapacity);
		Assert.assertEquals(count, (newCapacity - oldCapacity));
		for (int i = 0; i < 10; i++) {
			tfsname = tfsManager.saveFile(localFile, null, null);
			log.debug("@@ tfsname: " + tfsname);
			Assert.assertNotNull(tfsname);
			result = tfsManager.fetchFile(tfsname, null, "localfile");
			Assert.assertTrue(result);
			result = tfsManager.unlinkFile(tfsname, null);
			Assert.assertTrue(result);
		}
	}

	@Before
	public void setUp() {
		tfsManager = createTfsManager();
	}

	@After
	public void tearDown() {
		super.tearDown();
		TfsStatus tfsStatus = new TfsStatus();

		if (origRc != null && newRc != null && !origRc.equals("")
				&& newRc.equals("")) {
			tfsStatus.modifyRc(newRc, origRc);
		}

		for (String rc : addedRcs) {
			tfsStatus.removeRc(rc);
		}

		tfsStatus.setRcMode(RC_VALID_MODE);

		TimeUtility.sleep(MAX_UPDATE_TIME);
	}
}
