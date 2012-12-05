package com.taobao.common.tfs.testcase.function.meta;

import java.io.FileNotFoundException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class NameMetaManager_08_Save_File_Operation extends metaTfsBaseCase
{

	@Test
	public void testSave1KFile() 
	{
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("1K.jpg", "/save_file_1K");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave2MFile() 
	{
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("2M.jpg", "/save_file_2M");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave3MFile() 
	{
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("3M.jpg", "/save_file_3M");

		log.info("end: " + getCurrentFunctionName());
	}
	
	//@Test
	public void testSave6GFile()
	{
		log.info("begin: " + getCurrentFunctionName());
		
		testSaveFile("6G.jpg", "/save_file_6G");

		log.info("end: " + getCurrentFunctionName());
	}

	private void testSaveFile(String localFile, String tfsFile) 
	{
		String filePath = tfsFile + currentDateTime();

		TfsStatus tfsStatus = new TfsStatus();
		tfsManagerM = createtfsManager();
		tfsManagerM.rmFile(appId, userId, filePath);

		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long oldFileCount = tfsStatus.getFileCount(appKey);

		long localFileSize = 0;
		try 
		{
			localFileSize = FileUtility.getFileSize(localFile);
		}
		catch (FileNotFoundException e) 
		{
			log.debug("@@!! " + e.getMessage());
			Assert.assertTrue(false);
		}

		boolean result = tfsManagerM.saveFile(appId, userId, localFile, filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManagerM.getSessionId();
		log.debug("@!! first sessionId: " + sessionId);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 2) == localFileSize);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long newFileCount = tfsStatus.getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + localFileSize, newUsedCapacity);
		log.debug("@!! oldFileCount: " + oldFileCount + "; newFileCount: "+ newFileCount);

		tfsManagerM.destroy();

		tfsManagerM = createtfsManager();
		sessionId = tfsManagerM.getSessionId();
		log.debug("@!! second sessionId: " + sessionId);

		result = tfsManagerM.fetchFile(appId, userId, "localFile", filePath);
		Assert.assertTrue(result);

		TimeUtility.sleep(MAX_STAT_TIME);

		Assert.assertTrue(tfsStatus.getFileSize(sessionId, 1) == localFileSize);
		tfsManagerM.destroy();
	}
}
