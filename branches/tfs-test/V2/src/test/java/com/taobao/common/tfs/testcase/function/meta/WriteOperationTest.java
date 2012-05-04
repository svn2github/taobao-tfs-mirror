package com.taobao.common.tfs.testcase.function.meta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class WriteOperationTest extends BaseCase {
	
	@Test
	public void testWrite1KFile(){
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "1K.jpg";
		String filePath = "/test_1K_01" + currentDateTime();
		
		testWriteFile(localFile,filePath);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testWrite2MFile(){
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "2M.jpg";
		String filePath = "/test_2M_01" + currentDateTime();
		
		testWriteFile(localFile,filePath);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	@Test
	public void testWrite3MFile(){
		log.info("begin: " + getCurrentFunctionName());

		String localFile = "3M.jpg";
		String filePath = "/test_3M_01" + currentDateTime();
		
		testWriteFile(localFile,filePath);
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	private void testWriteFile(String localFile,String filePath){
		tfsManager = createTfsManager();
		TfsStatus tfsStatus = new TfsStatus();

		tfsManager.rmFile(appId, userId, filePath);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);

		long ret = tfsManager.createFile(appId, userId, filePath);
		Assert.assertEquals(ret, 0);

		long fileSize = 0;
		byte data[] = null;
		try {
			data = FileUtility.getByte(localFile);
			fileSize = FileUtility.getFileSize(localFile);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		
		ret = tfsManager.write(appId, userId, filePath, 0,data, 0, data.length);
		log.debug("ret is: "+ret);
		Assert.assertEquals(ret,data.length);
		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("sessionId: " + sessionId);
		
		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 2), fileSize);
		
		long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		Assert.assertEquals(oldUsedCapacity + fileSize, newUsedCapacity);
		tfsManager.destroy();
		
		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		ret = tfsManager.read(appId, userId, filePath, 0, data.length, output);
		Assert.assertEquals(ret, data.length);
		
		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertEquals(tfsStatus.getFileSize(sessionId, 1),fileSize);
		
		tfsManager.destroy();
	}
}
