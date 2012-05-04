package com.taobao.common.tfs.testcase.api.rc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;

import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TairRefCounter;
import com.taobao.common.tfs.utility.TimeUtility;
import com.taobao.common.tfs.utility.concurrent.ConcurrentExecutor;
import com.taobao.common.tfs.utility.concurrent.SaveFileOperation;
import com.taobao.common.tfs.utility.concurrent.UnlinkFileOperation;

public class UniqueTest extends BaseCase {
	private final int FILE_SIZE_100K = 100 * 1024;

	boolean executionResult = false;
	private TairRefCounter refCounter = null;
	private List<String> tfsNames = new ArrayList<String>();

	private Logger logger = Logger.getLogger(UniqueTest.class);

	@Test
	public void testSequentialSaveThreeSameFiles_01() {
		String localFile = getCurrentFunctionName()+".jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName = tfsManager.saveFile(localFile, null, null, false);

		logger.info("expected tfsName & refCounter: " + tfsName + ",1");
		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());
		Assert.assertEquals(refCounter.getRefCounter(), 1);
		Assert.assertEquals(refCounter.getTfsName(), tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		logger.info("expected tfsName & refCounter: " + tfsName + ",2");
		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());
		Assert.assertEquals(refCounter.getRefCounter(), 2);
		Assert.assertEquals(refCounter.getTfsName(), tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		logger.info("expected tfsName & refCounter: " + tfsName + ",3");
		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());
		Assert.assertEquals(refCounter.getRefCounter(), 3);
		Assert.assertEquals(refCounter.getTfsName(), tfsName);
	}

	@Test
	public void concurrentSaveTwoFilesTest_02() {
		String localFile = getCurrentFunctionName()+".jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		ConcurrentExecutor executorOne = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorOne.run();

		ConcurrentExecutor executorTwo = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorTwo.runWithDelay(10);

		List<Object> resultOne = executorOne.waitForComplete();
		List<Object> resultTwo = executorTwo.waitForComplete();
		String tfsName1 = (String) resultOne.get(0);
		String tfsName2 = (String) resultTwo.get(0);

		tfsNames.add(tfsName1);
		tfsNames.add(tfsName2);

		int count = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("Check information - tfsName1=" + tfsName1 + ";"
				+ "tfsName2=" + tfsName2 + "; refCounter=" + count
				+ ",refName=" + refName);

		Assert.assertTrue(tfsName1.equals(refName));
		Assert.assertFalse(tfsName1.equals(tfsName2));
		Assert.assertEquals(count, 1);

	}

	@Test
	public void concurrentSaveThreeFilesTest_03() {
		String localFile = "concurrentSaveThreeFilesTest_03.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		ConcurrentExecutor executorOne = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorOne.run();

		ConcurrentExecutor executorTwo = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 2);
		executorTwo.runWithDelay(10);

		List<Object> resultOne = executorOne.waitForComplete();
		List<Object> resultTwo = executorTwo.waitForComplete();
		String tfsName1 = (String) resultOne.get(0);
		String tfsName2 = (String) resultTwo.get(0);
		String tfsName3 = (String) resultTwo.get(1);

		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2,
				tfsName3 }));

		int count = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("Check information - tfsName1=" + tfsName1 + ";"
				+ "tfsName2=" + tfsName2 + ";tfsName3=" + tfsName3
				+ "; refCounter=" + count + ",refName=" + refName);

		Assert.assertTrue(tfsName1.equals(refName));
		Assert.assertFalse(tfsName1.equals(tfsName2));
		Assert.assertFalse(tfsName1.equals(tfsName3));
		Assert.assertFalse(tfsName2.equals(tfsName3));
		Assert.assertEquals(count, 1);

	}

	@Test
	public void saveFileWithHideTest_04() {
		String localFile = "saveFileWithHideTest_04.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		// Sequential save two same files, and it should be real unique
		String tfsName1 = tfsManager.saveFile(localFile, null, null, false);
		String refName = refCounter.getTfsName();

		tfsNames.add(tfsName1);

		Assert.assertTrue(refName.equals(tfsName1));
		Assert.assertEquals(refCounter.getRefCounter(), 1);

		String tfsName2 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName2)) {
			tfsNames.add(tfsName2);
		}

		Assert.assertTrue(refName.equals(tfsName2));
		Assert.assertEquals(refCounter.getRefCounter(), 2);

		// Hide the saved file and save one same file, it should be fake unique
		// It will return a different tfsName
		boolean ret = tfsManager.hideFile(refName, null, 1);
		Assert.assertTrue(ret);

		String tfsName3 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName3)) {
			tfsNames.add(tfsName3);
		}

		// unhide file and save the same file. It should be real unique
		// It will return one same tfsName
		ret = tfsManager.hideFile(refName, null, 0);
		Assert.assertTrue(ret);

		Assert.assertFalse(refName.equals(tfsName3));
		Assert.assertEquals(refCounter.getRefCounter(), 2);

		String tfsName4 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName4)) {
			tfsNames.add(tfsName4);
		}

		Assert.assertTrue(refName.equals(tfsName4));
		Assert.assertEquals(refCounter.getRefCounter(), 3);

		// Hide again and save one same file, it should be fake unique
		// It will return a different tfsName

		ret = tfsManager.hideFile(refName, null, 1);
		Assert.assertTrue(ret);

		String tfsName5 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName5)) {
			tfsNames.add(tfsName5);
		}

		// unhide in order to remove the saved files
		ret = tfsManager.hideFile(refName, null, 0);
		Assert.assertTrue(ret);

		Assert.assertFalse(refName.equals(tfsName5));
		Assert.assertFalse(tfsName3.equals(tfsName5));

		Assert.assertEquals(refCounter.getRefCounter(), 3);
	}

	@Test
	public void saveLargeFileWithSaveFileInterface_05() {
		String localFile = "saveLargeFileWithSaveFileInterface_05.jpg";

		setUpFixture(localFile, 1000 * FILE_SIZE_100K);

		String tfsName1 = tfsManager.saveFile(localFile, null, null, false);
		String tfsName2 = tfsManager.saveFile(localFile, null, null, false);

		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertFalse(tfsName1.equals(tfsName2));
	}

	@Test
	public void saveLargeFileWithSaveLargeFileInterface_06() {
		String localFile = "saveLargeFileWithSaveLargeFileInterface_06.jpg";

		setUpFixture(localFile, 20 * FILE_SIZE_100K);

		String tfsName1 = tfsManager.saveLargeFile(localFile, null, null);
		String tfsName2 = tfsManager.saveLargeFile(localFile, null, null);
		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertFalse(tfsName1.equals(tfsName2));
	}

	@Test
	public void saveFileWithSuffixTwoNulls_07() {
		String localFile = "saveFileWithSuffixTwoNulls_07.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName1 = tfsManager.saveFile(localFile, null, null);
		String tfsName2 = tfsManager.saveFile(localFile, null, null);

		int counter = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertTrue(tfsName1.equals(tfsName2));
		Assert.assertTrue(tfsName1.equals(refName));
		Assert.assertEquals(2, counter);
	}

	@Test
	public void saveFileWithOneSuffixOtherNull_08() {
		String localFile = "saveFileWithOneSuffixOtherNull_08.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix);
		String tfsName2 = tfsManager.saveFile(localFile, null, null);

		int counter = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertFalse(tfsName1.equals(tfsName2));
		Assert.assertTrue(refName.equals(tfsName1 + suffix));
		Assert.assertEquals(1, counter);
	}

	@Test
	public void saveFileWithOneNullOtherSuffix_09() {
		String localFile = "saveFileWithOneNullOtherSuffix_09.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, null);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix);

		int counter = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertFalse(tfsName1.equals(tfsName2));
		Assert.assertTrue(refName.equals(tfsName1));
		Assert.assertEquals(1, counter);
	}

	@Test
	public void saveFileWithTwoSameSuffix_10() {
		String localFile = "saveFileWithTwoSameSuffix_10.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix);

		int counter = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertTrue(tfsName1.equals(tfsName2));
		Assert.assertTrue(refName.equals(tfsName1 + suffix));
		Assert.assertEquals(2, counter);
	}

	@Test
	public void saveFileWithTwoDifferentSuffix_11() {
		String localFile = "saveFileWithTwoDifferentSuffix_11.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix0 = ".jpg0";
		String suffix1 = ".jpg1";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix0);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix1);

		int counter = refCounter.getRefCounter();
		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		Assert.assertFalse(tfsName1.equals(tfsName2));
		Assert.assertTrue(refName.equals(tfsName1 + suffix0));
		Assert.assertEquals(1, counter);
	}

	@Test
	public void unlinkSequentialSavedThreeSameFilesTest_12() {
		String localFile = "unlinkSequentialSavedThreeSameFilesTest_12.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		String refName = refCounter.getTfsName();
		Assert.assertEquals(refName, tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertEquals(refName, tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertEquals(refName, tfsName);

		boolean ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(2, refCounter.getRefCounter());

		ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName, null, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName, null, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertFalse(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());
	}

	@Test
	public void concurrentSaveTwoFilesUnlinkSecondTest_13() {
		String localFile = "concurrentSaveTwoFilesUnlinkSecondTest_13.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		ConcurrentExecutor executorOne = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorOne.run();

		ConcurrentExecutor executorTwo = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorTwo.runWithDelay(10);

		List<Object> resultOne = executorOne.waitForComplete();
		List<Object> resultTwo = executorTwo.waitForComplete();
		String tfsName1 = (String) resultOne.get(0);
		String tfsName2 = (String) resultTwo.get(0);

		tfsNames.add(tfsName1);
		tfsNames.add(tfsName2);

		boolean ret = tfsManager.unlinkFile(tfsName2, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertFalse(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);

		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("TEMP"));

		int count = refCounter.getRefCounter();
		Assert.assertEquals(count, 1);

	}

	@Test
	public void concurrentSaveThreeFilesTestUnlinkTheLastTwo_14() {
		String localFile = "concurrentSaveThreeFilesTestUnlinkTheLastTwo_14.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		ConcurrentExecutor executorOne = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 1);
		executorOne.run();

		ConcurrentExecutor executorTwo = new ConcurrentExecutor(
				new SaveFileOperation(tfsManager, localFile), 2);
		executorTwo.runWithDelay(10);

		List<Object> resultOne = executorOne.waitForComplete();
		List<Object> resultTwo = executorTwo.waitForComplete();
		String tfsName1 = (String) resultOne.get(0);
		String tfsName2 = (String) resultTwo.get(0);
		String tfsName3 = (String) resultTwo.get(1);

		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2,
				tfsName3 }));

		logger.debug("Check information - tfsName1=" + tfsName1 + ";"
				+ "tfsName2=" + tfsName2 + ";tfsName3=" + tfsName3 + ";"
				+ "refName=" + refCounter.getTfsName());

		boolean ret = tfsManager.unlinkFile(tfsName3, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);

		int count = refCounter.getRefCounter();
		Assert.assertEquals(1, count);

		ret = tfsManager.unlinkFile(tfsName2, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);

		count = refCounter.getRefCounter();
		Assert.assertEquals(count, 1);

		ret = tfsManager.unlinkFile(tfsName1, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);

		count = refCounter.getRefCounter();
		Assert.assertEquals(count, 0);

	}

	// @Test
	public void hideUnlink() {
		String localFile = "hideUnlink.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName1 = tfsManager.saveFile(localFile, null, null, false);

		boolean ret = tfsManager.hideFile(tfsName1, null, 1);
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName1, null);
		Assert.assertTrue(ret);

	}

	// @Test //It will fail, if unique is enabled, hidden file can not be
	// unlinked
	public void saveFileWithHideUnlinkTest_15() {
		String localFile = "saveFileWithHideUnlinkTest_15.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		// Sequential save two same files, and it should be real unique
		String tfsName1 = tfsManager.saveFile(localFile, null, null, false);
		String refName = refCounter.getTfsName();

		tfsNames.add(tfsName1);

		Assert.assertTrue(refName.equals(tfsName1));
		Assert.assertEquals(refCounter.getRefCounter(), 1);

		String tfsName2 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName2)) {
			tfsNames.add(tfsName2);
		}

		Assert.assertTrue(refName.equals(tfsName2));
		Assert.assertEquals(refCounter.getRefCounter(), 2);

		// Hide the saved file and unlink
		// The ref count will be decreased
		boolean ret = tfsManager.hideFile(refName, null, 1);
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(refName, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(refCounter.getRefCounter(), 1);

		// save file and it is fake unique
		String tfsName3 = tfsManager.saveFile(localFile, null, null, false);
		if (!tfsNames.contains(tfsName3)) {
			tfsNames.add(tfsName3);
		}

		ret = tfsManager.unlinkFile(tfsName3, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName3, null, "TEMP");
		Assert.assertFalse(ret);

		ret = tfsManager.fetchFileForce(refName, null, "TEMP");
		Assert.assertTrue(ret);

		Assert.assertEquals(refCounter.getRefCounter(), 1);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("TEMP"));

		// unhide file
		ret = tfsManager.hideFile(refName, null, 0);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(refName, null, "TEMP");
		Assert.assertTrue(ret);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("TEMP"));

		ret = tfsManager.unlinkFile(refName, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(refName, null, "TEMP");
		Assert.assertFalse(ret);

		Assert.assertEquals(refCounter.getRefCounter(), 0);
	}

	@Test
	public void saveFileWithSuffixTwoNullsUnlink_16() {
		String localFile = "saveFileWithSuffixTwoNullsUnlink_16.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName1 = tfsManager.saveFile(localFile, null, null);
		String tfsName2 = tfsManager.saveFile(localFile, null, null);

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		boolean ret = tfsManager.unlinkFile(tfsName1, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertTrue(ret);

		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("TEMP"));

		ret = tfsManager.unlinkFile(tfsName2, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertTrue(ret);
	}

	@Test
	public void saveFileWithOneSuffixOtherNullUnlink01_17() {
		String localFile = "saveFileWithOneSuffixOtherNullUnlink_17.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix);
		String tfsName2 = tfsManager.saveFile(localFile, null, null);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		boolean ret = tfsManager.unlinkFile(tfsName1, suffix);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName1, suffix, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName2, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, null, "TEMP");
		Assert.assertFalse(ret);
	}

	@Test
	public void saveFileWithOneSuffixOtherNullUnlink02_18() {
		String localFile = "saveFileWithOneSuffixOtherNullUnlink02_18.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix);
		String tfsName2 = tfsManager.saveFile(localFile, null, null);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		boolean ret = tfsManager.unlinkFile(tfsName2, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, suffix, "TEMP");
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.unlinkFile(tfsName1, suffix);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, suffix, "TEMP");
		Assert.assertTrue(ret);
	}

	@Test
	public void saveFileWithOneNullOtherSuffixUnlink01_19() {
		String localFile = "saveFileWithOneNullOtherSuffixUnlink01_19.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, null);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		boolean ret = tfsManager.unlinkFile(tfsName1, null);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, suffix, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName2, suffix);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, suffix, "TEMP");
		Assert.assertFalse(ret);
	}

	@Test
	public void saveFileWithOneNullOtherSuffixUnlink02_20() {
		String localFile = "saveFileWithOneNullOtherSuffixUnlink02_20.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, null);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));

		boolean ret = tfsManager.unlinkFile(tfsName2, suffix);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.unlinkFile(tfsName1, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, null, "TEMP");
		Assert.assertTrue(ret);
	}

	@Test
	public void saveFileWithTwoSameSuffixUnlink_21() {
		String localFile = "saveFileWithTwoSameSuffixUnlink_21.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix = ".jpg";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix);

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ "ref counter=" + refCounter.getRefCounter());
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1, tfsName2 }));
		
		Assert.assertEquals(2, refCounter.getRefCounter());
		
		boolean ret = tfsManager.unlinkFile(tfsName1, suffix);
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName2, suffix, "TEMP");
		Assert.assertTrue(ret);

		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("TEMP"));

		ret = tfsManager.unlinkFile(tfsName2, suffix);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName2, suffix, "TEMP");
		Assert.assertTrue(ret);
	}

	@Test
	public void saveFileWithTwoDifferentSuffixUnlink01_22() {
		String localFile = "saveFileWithTwoDifferentSuffixUnlink01_22.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix0 = ".jpg0";
		String suffix1 = ".jpg1";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix0);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix1);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName+";refCounter="+refCounter.getRefCounter());
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1 + suffix0,
				tfsName2 + suffix1 }));

		boolean ret = tfsManager.unlinkFile(tfsName1, suffix0);
		Assert.assertTrue(ret);
		Assert.assertEquals(0, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName1, suffix0, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, suffix1, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName2, suffix1);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName2, suffix1, "TEMP");
		Assert.assertFalse(ret);
	}

	@Test
	public void saveFileWithTwoDifferentSuffixUnlink02_23() {
		String localFile = "saveFileWithTwoDifferentSuffixUnlink02_23.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String suffix0 = ".jpg0";
		String suffix1 = ".jpg1";
		String tfsName1 = tfsManager.saveFile(localFile, null, suffix0);
		String tfsName2 = tfsManager.saveFile(localFile, null, suffix1);

		String refName = refCounter.getTfsName();

		logger.debug("tfsName1 = " + tfsName1 + "; tfsName2 = " + tfsName2
				+ ";refName=" + refName);
		tfsNames.addAll(Arrays.asList(new String[] { tfsName1 + suffix0,
				tfsName2 + suffix1 }));

		boolean ret = tfsManager.unlinkFile(tfsName2, suffix1);
		Assert.assertTrue(ret);
		Assert.assertEquals(1, refCounter.getRefCounter());

		ret = tfsManager.fetchFile(tfsName2, suffix1, "TEMP");
		Assert.assertFalse(ret);

		ret = tfsManager.fetchFile(tfsName1, suffix0, "TEMP");
		Assert.assertTrue(ret);

		ret = tfsManager.unlinkFile(tfsName1, suffix0);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName1, suffix0, "TEMP");
		Assert.assertTrue(ret);
	}

	@Test
	public void unlinkLargeFile_24() {
		String localFile = "unlinkLargeFile_24.jpg";

		setUpFixture(localFile, 20 * FILE_SIZE_100K);

		String tfsName = tfsManager.saveLargeFile(localFile, null, null);

		boolean ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);

		ret = tfsManager.fetchFile(tfsName, null, "TEMP");
		Assert.assertFalse(ret);
	}

	@Test
	public void restoreUnlinkFile_25() {
		String localFile = "restoreUnlinkFile_25.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName = tfsManager.saveFile(localFile, null, null, false);

		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());

		tfsManager.unlinkFile(tfsName, null);
		Assert.assertEquals(1, refCounter.getRefCounter());

		tfsManager.unlinkFile(tfsName, null, 0x2);

		logger.info("actual: " + refCounter.getTfsName() + ","
				+ refCounter.getRefCounter());
		Assert.assertEquals(2, refCounter.getRefCounter());
	}

	@Test
	public void concurrentUnlinkFile_26() {
		String localFile = "concurrentUnlinkFile_26.jpg";

		setUpFixture(localFile, FILE_SIZE_100K);

		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);

		Assert.assertEquals(3, refCounter.getRefCounter());

		ConcurrentExecutor executorOne = new ConcurrentExecutor(
				new UnlinkFileOperation(tfsManager, tfsName), 1);
		executorOne.run();

		ConcurrentExecutor executorTwo = new ConcurrentExecutor(
				new UnlinkFileOperation(tfsManager, tfsName), 1);
		executorTwo.runWithDelay(10);

		logger.debug("final ref counter: " + refCounter.getRefCounter());
		Assert.assertEquals(1, refCounter.getRefCounter());
	}
	
	
	//@Test
	public void testUnlinkFileWithTairConnectionBroken_27(){
		String localFile = getCurrentFunctionName()+".jpg";
		//NetworkUtility.blockNetwork("10.232.36.201", "10.232.12.0/24");
		//NetworkUtility.blockNetwork("10.232.36.201", "10.232.15.0/24");
		//setUpFixture(localFile, FILE_SIZE_100K);
		
		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);
		System.out.println("Tfs name: "+tfsName);
		//Assert.assertEquals(2, refCounter.getRefCounter());
		
		
		//NetworkUtility.blockPort("10.232.36.201", "10.232.12.141", 5198);
		TimeUtility.sleep(30);
		boolean ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);
		//NetworkUtility.freeNetwork("10.232.36.201");
		TimeUtility.sleep(10);
		
		System.out.println("counter=: "+refCounter.getRefCounter());
		//Assert.assertEquals(2, refCounter.getRefCounter());
	}
	
	//@Test
	public void testSaveFileWithTairConnectionBroken_28(){
		String localFile = getCurrentFunctionName()+".jpg";
		//NetworkUtility.blockNetwork("10.232.36.201", "10.232.12.0/24");
		//NetworkUtility.blockNetwork("10.232.36.201", "10.232.15.0/24");
		//setUpFixture(localFile, FILE_SIZE_100K);
		
		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);

		tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(tfsName);
		System.out.println("Tfs name: "+tfsName);
		//Assert.assertEquals(2, refCounter.getRefCounter());
		
		
		//NetworkUtility.blockPort("10.232.36.201", "10.232.12.141", 5198);
		TimeUtility.sleep(30);
		boolean ret = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(ret);
		//NetworkUtility.freeNetwork("10.232.36.201");
		TimeUtility.sleep(10);
		
		System.out.println("counter=: "+refCounter.getRefCounter());
		//Assert.assertEquals(2, refCounter.getRefCounter());
	}
	
	@Test
	public void testSaveFileRmFileWithMetaAndRc_29(){
		String localFile = getCurrentFunctionName()+".jpg";
		
		setUpFixture(localFile, FILE_SIZE_100K);
		
		tfsManager.rmFile(appId, userId, "/"+getCurrentFunctionName());
		
		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertEquals(1, refCounter.getRefCounter());
		Assert.assertEquals(tfsName, refCounter.getTfsName());
		
		logger.info("ref counter1: "+refCounter.getRefCounter());
		
		boolean result = tfsManager.saveFile(appId, userId, localFile,"/"+getCurrentFunctionName());
		Assert.assertTrue("Save File right path should be true", result);
		
		logger.info("ref counter2: "+refCounter.getRefCounter());
		Assert.assertEquals(1, refCounter.getRefCounter());
		
		int rmResult = tfsManager.rmFile(appId, userId, "/"+getCurrentFunctionName());
		Assert.assertEquals(0, rmResult);
		
		result = tfsManager.fetchFile(appId, userId, resourcesPath+"temp1","/"+getCurrentFunctionName());
		Assert.assertFalse(result);
		
		result = tfsManager.fetchFile(tfsName, null, "TEMP2");
		Assert.assertTrue(result);
		
		logger.info("ref counter3: "+refCounter.getRefCounter());
		Assert.assertEquals(1, refCounter.getRefCounter());
		
	}
	
	@Test
	public void testSaveFileRmFileWithMetaAndRc_30(){
		String localFile = getCurrentFunctionName()+".jpg";
		
		setUpFixture(localFile, FILE_SIZE_100K);
		
		tfsManager.rmFile(appId, userId, "/"+getCurrentFunctionName());
		
		boolean result = tfsManager.saveFile(appId, userId, localFile,"/"+getCurrentFunctionName());
		Assert.assertTrue("Save File right path should be true", result);
		
		logger.info("ref counter1: "+refCounter.getRefCounter());
		
		String tfsName = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertEquals(1, refCounter.getRefCounter());
		Assert.assertEquals(tfsName, refCounter.getTfsName());
		
		
		int rmResult = tfsManager.rmFile(appId, userId, "/"+getCurrentFunctionName());
		Assert.assertEquals(0, rmResult);
		
		result = tfsManager.fetchFile(appId, userId, resourcesPath+"temp1","/"+getCurrentFunctionName());
		Assert.assertFalse(result);
		
		result = tfsManager.fetchFile(tfsName, null, "TEMP2");
		Assert.assertTrue(result);
		Assert.assertEquals(1, refCounter.getRefCounter());
		logger.info("ref counter3: "+refCounter.getRefCounter());
	}
	
	
	private void setUpFixture(String fileName, int size) {
		FileInputStream input = null;

		try {
			FileUtility.generateRadomFile(fileName, size);
			input = new FileInputStream(new File(fileName));
			refCounter = new TairRefCounter(tfsManager);
			refCounter.setFileInputStream(input);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@After
	public void tearDown() {
		if (refCounter != null) {
			int unlinkTimes = 0;
			int totalCounter = refCounter.getRefCounter();

			while (refCounter.getRefCounter() > 0
					&& unlinkTimes <= totalCounter) {
				logger.info("unlink file: " + refCounter.getTfsName() + ", "
						+ "current counter: " + refCounter.getRefCounter());
				unlinkTimes++;
				tfsManager.unlinkFile(refCounter.getTfsName(), null);
			}

			if (unlinkTimes > totalCounter) {
				logger.error("unlink file error: " + refCounter.getTfsName());
				Assert.assertTrue(false);
			}
		}

		for (String each : tfsNames) {
			logger.info("unlink file: " + each);
			tfsManager.unlinkFile(each, null);
		}
	}
}
