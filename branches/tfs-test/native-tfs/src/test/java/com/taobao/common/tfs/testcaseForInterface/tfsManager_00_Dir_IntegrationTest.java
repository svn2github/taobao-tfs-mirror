package com.taobao.common.tfs.testcaseForInterface;

import com.taobao.common.tfs.namemeta.FileMetaInfo;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;

public class tfsManager_00_Dir_IntegrationTest extends tfsNameBaseCase {

	@Test
	public void test_01_happypath() {

		/* creat dir */

		boolean bRet;
		log.info("test_01_creat_dir");
		bRet = tfsManager.createDir(userId, "/text");
		Assert.assertTrue("Create Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_01_ls_Dir_First");
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList = null;
		FileMetaInfo metaInfo;
		int num = 11;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		for (int i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_01_ls_Dir_First");

		/* mv dir */

		log.info("test_01_mvDir_right");
		bRet = tfsManager.createDir (userId, "/text1");
		bRet = tfsManager.mvDir (userId, "/text1", "/text2");
		Assert.assertTrue("mvDir with right path should be true", bRet);
		tfsManager.rmDir (userId, "/text2");
		bRet = tfsManager.rmDir (userId, "/text1");
		Assert.assertFalse("mvDir with remove path should be false", bRet);

		/* ls dir */

		log.info("test_01_ls_Dir_Twice");
		metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		for (int i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_01_ls_Dir__Twice");

		/* rm dir */

		log.info("test_01_rmDir_right_filePath");
		tfsManager.createDir (userId, "/text");
		bRet = tfsManager.rmDir (userId, "/text");
		Assert.assertTrue("Remove Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_01_ls_Dir_Third");
		// metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());

		for (int i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_01_ls_Dir_Third");
	}

	@Test
	public void test_02_ls() {

		/* creat dir */

		boolean bRet;
		log.info("test_02_creat_dir");
		bRet = tfsManager.createDir (userId, "/text");
		Assert.assertTrue("Create Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_02_ls_Dir_First");
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList = null;
		FileMetaInfo metaInfo;
		int num = 11;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		int i;
		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_02_ls_Dir_First_End");
		/* rm dir */

		log.info("test_02_rmDir_right_filePath");
		tfsManager.createDir (userId, "/text");
		bRet = tfsManager.rmDir (userId, "/text");
		Assert.assertTrue("RM Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_02_ls_Dir_Twice");
		metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());

		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_02_ls_Dir_twice_End");

		/* mv dir */

		log.info("test_02_mvDir_right");
		bRet = tfsManager.createDir (userId, "/text1");
		bRet = tfsManager.mvDir (userId, "/text1", "/text2");
		Assert.assertTrue("mvDir with right path should be true", bRet);
		tfsManager.rmDir (userId, "/text2");
		bRet = tfsManager.rmDir (userId, "/text1");
		Assert.assertFalse("mvDir with remove path should be false", bRet);

		/* ls dir */

		log.info("test_02_ls_Third_Dir");
		metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_02_ls_Dir_Third_End");
	}

	@Test
	public void test_03_mv() {

		/* creat dir */

		boolean bRet;
		log.info("test_03_creat_dir");
		bRet = tfsManager.createDir (userId, "/text");
		Assert.assertTrue("Create Dir with right path should be true", bRet);

		/* mv dir */
		log.info("test_03_mvDir_right");
		bRet = tfsManager.createDir (userId, "/text1");
		bRet = tfsManager.mvDir (userId, "/text1", "/text2");
		Assert.assertTrue("mvDir with right path should be true", bRet);

		tfsManager.rmDir (userId, "/text2");
		bRet = tfsManager.rmDir (userId, "/text1");
		Assert.assertFalse("mvDir with remove path should be false", bRet);

		/* ls dir */

		log.info("test_03_ls_Dir_First");
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList = null;
		FileMetaInfo metaInfo;
		int num = 11;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		int i;
		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_03_ls_Dir");

		/* rm dir */

		log.info("test_03_rmDir_right_filePath");
		tfsManager.createDir (userId, "/text");
		bRet = tfsManager.rmDir (userId, "/text");
		Assert.assertTrue("Remove Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_03_ls_Dir_Twice");
		metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());

		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}

		log.info("test_03_ls_Dir_Twice_End");

	}

	@Test
	public void test_04_rm() {

		/* creat dir */

		boolean bRet;
		log.info("test_04_creat_dir");
		bRet = tfsManager.createDir (userId, "/text");
		Assert.assertTrue("Create Dir with right path should be true", bRet);

		/* rm dir */

		log.info("test_04_rmDir_right_filePath");
		tfsManager.createDir (userId, "/text");
		bRet = tfsManager.rmDir (userId, "/text");
		Assert.assertTrue("Remove Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_04_ls_Dir_First");
		List<FileMetaInfo> metaInfoList = new ArrayList<FileMetaInfo>();
		metaInfoList = null;
		FileMetaInfo metaInfo;
		int num = 11;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());
		int i;
		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_04_ls_Dir_First");

		/* mv dir */

		log.info("test_04_mvDir_right");
		bRet = tfsManager.createDir (userId, "/text1");
		bRet = tfsManager.mvDir (userId, "/text1", "/text2");
		Assert.assertTrue("mvDir with right path should be true", bRet);
		tfsManager.rmDir (userId, "/text2");
		bRet = tfsManager.rmDir (userId, "/text1");
		Assert.assertFalse("mvDir with remove path should be false", bRet);

		/* rm dir */

		log.info("test_04_rmDir_right_filePath");
		tfsManager.createDir (userId, "/text");
		bRet = tfsManager.rmDir (userId, "/text");
		Assert.assertTrue("Remove Dir with right path should be true", bRet);

		/* ls dir */

		log.info("test_04_ls_Dir_Twice");
		metaInfoList = null;
		metaInfoList = tfsManager.lsDir (userId, "/text1");
		Assert.assertNotNull(metaInfoList);
		Assert.assertEquals(num, metaInfoList.size());

		for (i = 0; i < num; i++) {
			log.info("The" + i + "file");
			metaInfo = metaInfoList.get(i);
			log.info("The fileName is" + metaInfo.getFileName());
			log.info("The pid is" + metaInfo.getPid());
			log.info("The id is" + metaInfo.getId());
			log.info("The length is" + metaInfo.getLength());
			log.info("*****************************************************");
		}
		log.info("test_04_ls_Dir_Twice_End");

	}

	@Before
	public void Before() {
		int i, j;
		for (i = 1; i <= 5; i++) {
			tfsManager.createDir (userId, "/text" + i);
			tfsManager.createFile (userId, "/textFile" + i);
		}
		for (i = 1; i <= 5; i++)
			for (j = 1; j <= 5; j++) {
				tfsManager.createDir (userId, "/text" + i + "/text" + j);
				tfsManager.createFile (userId, "/text" + i + "/textFile"
						+ j);
			}
	}

	@After
	public void After() {
		int i, j;
		for (i = 1; i <= 5; i++)
			for (j = 1; j <= 5; j++) {
				tfsManager.rmDir (userId, "/text" + i + "/text" + j);
				tfsManager.rmFile (userId, "/text" + i + "/textFile" + j);
			}

		for (i = 1; i <= 5; i++) {
			tfsManager.rmDir (userId, "/text" + i);
			tfsManager.rmFile (userId, "/textFile" + i);
		}

	}
}
