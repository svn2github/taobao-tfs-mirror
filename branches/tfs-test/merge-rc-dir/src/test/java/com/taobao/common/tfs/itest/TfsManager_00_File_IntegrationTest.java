package com.taobao.common.tfs.itest;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.junit.Ignore;
import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class TfsManager_00_File_IntegrationTest extends tfsNameBaseCase {

	@Test
	public void test_01_happypath() throws IOException {

		/* creat file */

		boolean bRet;
		log.info("test_01_createFile");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_01_lsFile_right_filePath_First");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_01_write_right");
		tfsManager.createFile(appId, userId, "/text");
		int localCrc = getCrc(resourcesPath + "10K.jpg");
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		long len = data.length;
		long offset = 0;
		long dataOffset = 0;
		long Ret;
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		int TfsCrc = getCrc(resourcesPath + "temp");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_01_lsFile_right_filePath_Twice");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_01_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* read file */

		log.info("test_01_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		int localcrc;
		int readcrc;
		localcrc = getCrc(resourcesPath + "2b");
		FileInputStream input = new FileInputStream(resourcesPath);
		System.out.println(input);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_01_lsFile_right_filePath_Third");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_01_write_rightTwice");
		tfsManager.createFile(appId, userId, "/text");
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_01_lsFile_right_filePath_Fourth");

		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_01_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");

		localcrc = getCrc(resourcesPath + "2b");
		System.out.println(input);

		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_01_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* read file */

		log.info("test_01_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		localcrc = getCrc(resourcesPath + "2b");
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_01_write_right_Twice");
		tfsManager.createFile(appId, userId, "/text");
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* save file */

		log.info("test_01_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_02_read() throws IOException {

		/* creat file */

		boolean bRet;
		log.info("test_02_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_02_read_right_First");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		int localcrc;
		int readcrc;
		localcrc = getCrc(resourcesPath + "2b");
		FileInputStream input = new FileInputStream(resourcesPath);
		System.out.println(input);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);
		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_02_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* read file */

		log.info("test_02_read_right_twice");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");

		localcrc = getCrc(resourcesPath + "2b");
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_02_lsFile_right_filePath");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_02_write_right");
		tfsManager.createFile(appId, userId, "/text");
		int localCrc = getCrc(resourcesPath + "10K.jpg");
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		long len = data.length;
		long offset = 0;
		long dataOffset = 0;
		long Ret;
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		int TfsCrc = getCrc(resourcesPath + "temp");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_02_fetchFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_02_read_right_Fourth");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		localcrc = getCrc(resourcesPath + "2b");
		System.out.println(input);
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);
		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_02_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* read file */

		log.info("test_02_read_right_Fifth");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		localcrc = getCrc(resourcesPath + "2b");
		System.out.println(input);
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);
		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_03write() throws IOException {

		/* creat file */

		boolean bRet;
		log.info("test_03_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_03_write_right_First");
		tfsManager.createFile(appId, userId, "/text");
		int localCrc = getCrc(resourcesPath + "10K.jpg");
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		long len = data.length;
		long offset = 0;
		long dataOffset = 0;
		long Ret;
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		int TfsCrc = getCrc(resourcesPath + "temp");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_03_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		int localcrc;
		int readcrc;
		localcrc = getCrc(resourcesPath + "2b");
		FileInputStream input = new FileInputStream(resourcesPath);
		System.out.println(input);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_03_lsFile_right_filePath");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_03_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* write file */

		log.info("test_03_write_right_twice");
		tfsManager.createFile(appId, userId, "/text");
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_03_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		localcrc = getCrc(resourcesPath + "2b");
		System.out.println(input);
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);
		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* save file */

		log.info("test_03_saveFile_right");

		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_03_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* fetch file */

		log.info("test_03_fetchFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_04ls() throws IOException {

		/* creat file */

		boolean bRet;
		log.info("test_04_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_04_lsFile_First");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_04_write_right");
		tfsManager.createFile(appId, userId, "/text");
		int localCrc = getCrc(resourcesPath + "10K.jpg");
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		long len = data.length;
		long offset = 0;
		long dataOffset = 0;
		long Ret;
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		int TfsCrc = getCrc(resourcesPath + "temp");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_04_read_right");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		int localcrc;
		int readcrc;
		localcrc = getCrc(resourcesPath + "2b");
		FileInputStream input = new FileInputStream(resourcesPath);
		System.out.println(input);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_04_lsFile_Twice");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_04_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* ls file */

		log.info("test_04_lsFile_Third");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_04_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* ls file */

		log.info("test_04_lsFile_Fourth");

		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* save file */

		log.info("test_04_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_04_lsFile_Fifth");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_04_fetchFile_right");

		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_04_lsFile_Sixth");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	/* wrong order */
	public void test_05rm() throws IOException {

		/* creat file */

		boolean bRet;
		log.info("test_05_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_05_lsFile_right_filePath_First");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* write file */

		log.info("test_05_write_right");
		tfsManager.createFile(appId, userId, "/text");
		int localCrc = getCrc(resourcesPath + "10K.jpg");
		byte data[] = null;
		data = getByte(resourcesPath + "10K.jpg");
		Assert.assertNotNull(data);
		long len = data.length;
		long offset = 0;
		long dataOffset = 0;
		long Ret;
		Ret = tfsManager.write(appId, userId, "/text", offset, data,
				dataOffset, len);
		Assert.assertEquals(Ret, len);
		tfsManager.fetchFile(appId, userId, resourcesPath + "temp", "/text");
		int TfsCrc = getCrc(resourcesPath + "temp");
		Assert.assertEquals(TfsCrc, localCrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_05_read_right_First");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		int localcrc;
		int readcrc;
		localcrc = getCrc(resourcesPath + "2b");
		FileInputStream input = new FileInputStream(resourcesPath);
		System.out.println(input);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);

		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_05_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* ls file */

		log.info("test_05_lsFile_right_filePath_Twice");
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* read file */

		log.info("test_05_read_right_Twice");
		tfsManager.createFile(appId, userId, "/text");
		tfsManager.saveFile(appId, userId, resourcesPath + "2b", "/text");
		localcrc = getCrc(resourcesPath + "2b");
		System.out.println(input);
		Assert.assertEquals(2,
				tfsManager.read(appId, userId, "/text", 0, 2, output));
		readcrc = getCrc(output);
		System.out.println(output.toByteArray());
		Assert.assertEquals(localcrc, readcrc);
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_05_fetchFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* save file */

		log.info("test_05_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_06mv() {

		/* creat file */

		boolean bRet;
		log.info("test_06_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_06_lsFile_right_filePath_First");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_06_mvFile_right_First");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* ls file */

		log.info("test_06_lsFile_right_filePath_Twice");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_06_fetchFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_06_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* mv file */

		log.info("test_06_mvFile_right_Twice");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* ls file */

		log.info("test_06_lsFile_right_filePath_Third");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_07save() {

		/* creat file */

		boolean bRet;
		log.info("test_07_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_07_lsFile_right_filePath");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_07_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* save file */

		log.info("test_07_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_07_fetchFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_07_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_07_lsFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* save file */

		log.info("test_07_saveFile_right");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertFalse("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertFalse("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_07_lsFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");
	}

	@Test
	public void test_08fetch() {

		/* creat file */

		boolean bRet;
		log.info("test_08_createFile_right_filePath");
		bRet = tfsManager.createFile(appId, userId, "/text");
		Assert.assertTrue("Create File with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text");

		/* ls file */

		log.info("test_08_lsFile_right_filePath");
		FileMetaInfo metaInfo;
		metaInfo = null;
		tfsManager.createFile(appId, userId, "/text");
		metaInfo = tfsManager.lsFile(appId, userId, "/text");
		Assert.assertNotNull(metaInfo);
		log.info("The fileName is" + metaInfo.getFileName());
		log.info("The pid is" + metaInfo.getPid());
		log.info("The id is" + metaInfo.getId());
		log.info("The length is" + metaInfo.getLength());
		log.info("*****************************************************");
		tfsManager.rmFile(appId, userId, "/text");

		/* fetch file */

		log.info("test_08_fetchFile_right_First");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* mv file */

		log.info("test_08_mvFile_right");
		bRet = tfsManager.createFile(appId, userId, "/text1");
		bRet = tfsManager.mvFile(appId, userId, "/text1", "/text2");
		Assert.assertTrue("mvFile with right path should be true", bRet);
		tfsManager.rmFile(appId, userId, "/text2");
		bRet = tfsManager.rmFile(appId, userId, "/text1");
		Assert.assertFalse("mvFile with remove path should be false", bRet);

		/* fetch file */

		log.info("test_08_fetchFile_right_Twice");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");

		/* rm file */

		log.info("test_08_rmFile_right_filePath");
		tfsManager.createFile(appId, userId, "/text");
		bRet = tfsManager.rmFile(appId, userId, "/text");
		Assert.assertTrue("Remove File with right path should be true", bRet);

		/* fetch file */

		log.info("test_08_fetchFile_right_Third");
		bRet = tfsManager.saveFile(appId, userId, resourcesPath + "100K.jpg",
				"/text");
		Assert.assertTrue("Save File right path should be true", bRet);
		bRet = tfsManager.fetchFile(appId, userId, resourcesPath + "temp",
				"/text");
		Assert.assertTrue("Fetch File right path should be true", bRet);
		Assert.assertEquals(getCrc(resourcesPath + "temp"),
				getCrc(resourcesPath + "100K.jpg"));
		tfsManager.rmFile(appId, userId, "/text");
	}

}