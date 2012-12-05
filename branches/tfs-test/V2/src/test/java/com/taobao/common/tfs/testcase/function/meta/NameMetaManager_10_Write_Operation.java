package com.taobao.common.tfs.testcase.function.meta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class NameMetaManager_10_Write_Operation extends metaTfsBaseCase
{

	@Test
	public void testWrite1KFile() 
	{
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "1K.jpg";
		String filePath = "/test_1K_01" + currentDateTime();

		testWriteFile(localFile, filePath);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWrite2MFile() 
	{
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";
		String filePath = "/test_2M_01" + currentDateTime();

		testWriteFile(localFile, filePath);

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testWrite3MFile()
	{
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "3M.jpg";
		String filePath = "/test_3M_01" + currentDateTime();

		testWriteFile(localFile, filePath);

		log.info("end: " + getCurrentFunctionName());
	}

	private void testWriteFile(String localFile, String filePath)
	{
		tfsManagerM = createtfsManager();
		TfsStatus tfsStatus = new TfsStatus();

		tfsManagerM.rmFile(appId, userId, filePath);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);

		long ret = tfsManagerM.createFile(appId, userId, filePath);
		Assert.assertEquals(ret, 0);

		long fileSize = 0;
		byte data[] = null;
		try 
		{
			data = FileUtility.getByte(localFile);
			fileSize = FileUtility.getFileSize(localFile);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}

		ret = tfsManagerM.write(appId, userId, filePath, 0, data, 0, data.length);
		log.debug("ret is: " + ret);
		Assert.assertEquals(ret, data.length);
		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManagerM.getSessionId();
		log.debug("sessionId: " + sessionId);

		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2), fileSize);

		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		Assert.assertEquals(oldUsedCapacity + fileSize, newUsedCapacity);
		tfsManagerM.destroy();

		tfsManagerM = createtfsManager();
		sessionId = tfsManagerM.getSessionId();

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		ret = tfsManagerM.read(appId, userId, filePath, 0, data.length, output);
		Assert.assertEquals(ret, data.length);

		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 1), fileSize);

		tfsManagerM.destroy();
	}
}
