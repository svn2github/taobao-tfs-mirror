package com.taobao.common.tfs.itest;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.tfsNameBaseCase;
import com.taobao.common.tfs.impl.FSName;

public class tfsManager_12_saveFile extends tfsNameBaseCase {
	
	@Before
	public void setup(){
		sleep(100);
		tfsManager.rmFile(appId, userId, "/text");
		tfsManager.rmFile(appId, userId, "/text1");
		
		tfsManager.rmDir(appId, userId, "/text112");
	}
	
	@After
	public void teardown(){
		sleep(10);
	}
	
	public String getFileName(){
		return null;
	}
	
	@Test
	public void test_00_saveFile_right() throws InterruptedException {
		log.info("test_01_saveFile_right");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text1");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text1");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text1");
	}

	@Test
	public void test_01() {
		/* save file */
		boolean bRet = false;
		log.info("test_01");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text1");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text1");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text1");
	}

	@Test
	public void test_02_saveFile_null_localFile() {
		log.info("test_02_saveFile_null_localFile");
		boolean bRet = false;
		log.info("test_02_saveFile_null_localFile");
		// bRet=tfsManager.saveFile(appId, userId, null,"/text2/test111");
		// Assert.assertFalse("Save File null path should be false", bRet);
	}

	@Test
	public void test_03_saveFile_empty_localFile() {
		log.info("test_03_saveFile_empty_localFile");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, "", "/text2");
		Assert.assertFalse("Save File empty path should be false", bRet);
	}

	@Test
	public void test_04_saveFile_wrong_localFile() {
		log.info("test_04_saveFile_wrong_localFile");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, "sdklafhksdh", "/text4");
		Assert.assertFalse("Save File wrong path should be false", bRet);
	}

	@Test
	public void test_05_saveFile_null_fileName() {
		log.info("test_05_saveFile_null_fileName");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				null);
		Assert.assertFalse("Save File null fileName should be false", bRet);
	}

	@Test
	public void test_06_saveFile_empty_fileName() {
		log.info("test_06_saveFile_empty_fileName");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"");
		Assert.assertFalse("Save File null fileName should be false", bRet);
	}

	@Test
	public void test_07_saveFile_wrong_fileName_1() {
		log.info("test_07_saveFile_wrong_fileName_1");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text4/");
		Assert.assertFalse("Save File wrong 1 fileName should be false", bRet);
	}

	@Test
	public void test_08_saveFile_wrong_fileName_2() {
		log.info("test_08_saveFile_wrong_fileName_2");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"text8");
		Assert.assertFalse("Save File wrong 2 fileName should be false", bRet);
	}

	@Test
	public void test_09_saveFile_wrong_fileName_3() {
		log.info("test_09_saveFile_wrong_fileName_3");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/");
		Assert.assertFalse("Save File wrong 3 fileName should be false", bRet);
	}

	@Test
	public void test_10_saveFile_leap_fileName() {
		log.info("test_10_saveFile_leap_fileName");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text10/text");
		Assert.assertFalse("Save File leap fileName should be false", bRet);
	}

	@Test
	public void test_11_saveFile_with_Dir() {
		log.info("test_11_saveFile_with_Dir");
		boolean bRet;
		bRet = tfsManager.createDir(appId, userId, "/text1");
		assertTrue(bRet);
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text1");
		Assert.assertFalse("Save File with the same name Dir should be true",
				bRet);
		tfsManager.rmDir(appId, userId, "/text1");
		tfsManager.rmFile(appId, userId, "/text1");
	}

	//@Test
	public void test_12_saveFile_large() {
		log.info("test_12_saveFile_large");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "1G.jpg",
				"/text12");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text12");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "1G.jpg"));
		tfsManager.rmFile(appId, userId, "/text12");
	}

	@Test
	public void test_13_saveFile_many_times() {
		log.info("test_13_saveFile_many_times");
		boolean bRet;
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text13");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text13");
		Assert.assertFalse("Save File two times should be false", bRet);
		tfsManager.rmFile(appId, userId, "/text13");
	}

	/*
	 * *This case is for bug
	 * java-client-dev-2.2.3——客户端执行带后缀写文件时返回的fileid对于相同的后缀总是一样的 bug 145786
	 */
	//@Test
	public void test_14() throws Exception {
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);

		String sFileName1 = null;
		sFileName1 = tfsManager.saveFile(data, null, "jpg");

		String sFileName2 = null;
		sFileName2 = tfsManager.saveFile(data, null, "jpg");

		assertTrue(sFileName1 != null);
		assertTrue(sFileName2 != null);

		try {
			FSName fsName1 = new FSName(sFileName1, "jpg");
			FSName fsName2 = new FSName(sFileName2, "jpg");
			log.info("fileId is :" + fsName1.getFileId());
			log.info("fileId is :" + fsName2.getFileId());
			assertTrue(fsName1.getFileId() != fsName2.getFileId());
		} catch (TfsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
