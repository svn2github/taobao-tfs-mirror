package com.taobao.common.tfs.testcase.function.rc;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class Md5Test extends rcTfsBaseCase {


	

	 @Test
		public void testSaveLargeFileByByteFetchFile1K() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("1K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}	
	 @Test
		public void testSaveLargeFileByByteFetchFile2M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("2M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}	
	 @Test
		public void testSaveLargeFileByByteFetchFile5M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("5M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFile10M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("10M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}



private void testSaveLargeFileByByteFetchFile(String localFile) throws IOException {
	//get localfile md5
	StringBuilder fileMd5before = super.fileMd5(localFile);
	TfsStatus tfsStatus = new TfsStatus();
	DefaultTfsManager tfsManager = createTfsManager();
	byte data[]=null;
	data=getByte(localFile);
	
	TimeUtility.sleep(MAX_STAT_TIME);
	String tfsName = "";
	tfsName=tfsManager.saveLargeFile(null,null,data,0,data.length,key);	
	Assert.assertNotNull(tfsName);
	tfsNames.add(tfsName);
	log.debug("Saved tfsname is: " + tfsName);	
	tfsManager.destroy();
	tfsManager = createTfsManager();
	boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
	//asert localfile md5 and serverfile md5
	StringBuilder fileMd5after = super.fileMd5("localfile");
	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
	Assert.assertTrue(result);
	tfsManager.destroy();

}
//
//private void testSaveLargeFileByByteFetchFileByByte(String localFile) throws IOException {
//	//get byte md5
//	byte data[]=null;
//	data=getByte(localFile);
//	
//	String fileMd5before = super.md5(data);
//	TfsStatus tfsStatus = new TfsStatus();
//	DefaultTfsManager tfsManager = createTfsManager();
//	
//	
//	TimeUtility.sleep(MAX_STAT_TIME);
//	String tfsName = "";
//	tfsName=tfsManager.saveLargeFile(null,null,data,0,data.length,key);	
//	Assert.assertNotNull(tfsName);
//	tfsNames.add(tfsName);
//	log.debug("Saved tfsname is: " + tfsName);	
//	ByteArrayOutputStream output = new ByteArrayOutputStream();
//	boolean result = tfsManager.fetchFile(tfsName, null, output);
//	log.debug(result+"todaytest");
//	Assert.assertTrue(result);	
//	//asert localfile md5 and serverfile md5
//	String fileMd5after = super.md5(output.toByteArray().clone());
//	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
//	Assert.assertTrue(result);
//	tfsManager.destroy();
//
//}
//
////saveFile(data, null, null); 5m文件上传失败
//private void testSaveFileByByteFetchFile(String localFile) throws IOException {
//	//get localfile md5
//		StringBuilder fileMd5before = super.fileMd5(localFile);
//		TfsStatus tfsStatus = new TfsStatus();
//		DefaultTfsManager tfsManager = createTfsManager();
//		byte data[]=null;
//		data=getByte(localFile);		
//		TimeUtility.sleep(MAX_STAT_TIME);
//
//		String tfsName = "";
//		tfsName=tfsManager.saveFile(data, null, null);
//		Assert.assertNotNull(tfsName);
//		tfsNames.add(tfsName);
//		log.debug("Saved tfsname is: " + tfsName);	
//		tfsManager.destroy();
//		tfsManager = createTfsManager();
//		boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
//		//asert localfile md5 and serverfile md5
//		StringBuilder fileMd5after = super.fileMd5("localfile");
//		Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
//		Assert.assertTrue(result);
//		tfsManager.destroy();
//
//	}
//
////saveFile(data, null, null); 5m文件上传失败
//
//
//private void testSaveFileByByteFetchFileByByte(String localFile) throws IOException {
//	//get byte md5
//	byte data[]=null;
//	data=getByte(localFile);
//	String fileMd5before = super.md5(data);
//	TfsStatus tfsStatus = new TfsStatus();
//	DefaultTfsManager tfsManager = createTfsManager();
//	
//	TimeUtility.sleep(MAX_STAT_TIME);
//	String tfsName = "";
//	tfsName=tfsManager.saveFile(data,null,null);	
//	Assert.assertNotNull(tfsName);
//	tfsNames.add(tfsName);
//	log.debug("Saved tfsname is: " + tfsName);	
//	ByteArrayOutputStream output = new ByteArrayOutputStream();
//	boolean result = tfsManager.fetchFile(tfsName, null, output);
//	log.debug(result);
//	Assert.assertTrue(result);	
//	//assert localbyte md5 and serverbyte md5
//	String fileMd5after = super.md5(output.toByteArray().clone());
//	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
//	Assert.assertTrue(result);
//	tfsManager.destroy();
//
//}
//
//
//private void testSaveFileByByteWithOffsetLengthFetchFileByByte(String localFile) throws IOException {
//	//get byte md5
//	byte data[]=null;
//	byte dataOffset[]=null;
//	data=getByte(localFile);
//	dataOffset =Arrays.copyOfRange(data,9,69);
//	
//	String fileMd5before = super.md5(dataOffset);
//	TfsStatus tfsStatus = new TfsStatus();
//	DefaultTfsManager tfsManager = createTfsManager();
//	
//	
//	long fileLength = 0;
//	try {
//		fileLength = FileUtility.getFileSize(localFile);
//	} catch (FileNotFoundException e) {
//		e.printStackTrace();
//		Assert.assertTrue(false);
//	}
//
//	String tfsName = "";
//	if (fileLength > (2 * (1 << 20))) {
//		tfsName = tfsName=tfsManager.saveLargeFile(null,null,data,10,60,key);	
//	} else {
//		tfsName=tfsManager.saveFile(null,null,data,10,60,false);
//	}
//	
//	TimeUtility.sleep(MAX_STAT_TIME);
//	Assert.assertNotNull(tfsName);
//	tfsNames.add(tfsName);
//	log.debug("Saved tfsname is: " + tfsName);	
//	ByteArrayOutputStream output = new ByteArrayOutputStream();
//	boolean result = tfsManager.fetchFile(tfsName, null, output);
//	log.debug(result);
//	Assert.assertTrue(result);	
//	//assert localbyte md5 and serverbyte md5
//	String fileMd5after = super.md5(output.toByteArray().clone());
//	System.out.println(fileMd5after);
//	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
//	Assert.assertTrue(result);
//	tfsManager.destroy();
//
//}
//
//
//private void testSaveFileByByteWithOffsetLengthFetchFile(String localFile) throws IOException {
//	//get byte md5
//		byte data[]=null;
//		byte dataOffset[]=null;
//		data=getByte(localFile);
//		dataOffset =Arrays.copyOfRange(data,9,69);						
//		String fileMd5before = super.md5(dataOffset);
//		TfsStatus tfsStatus = new TfsStatus();
//		DefaultTfsManager tfsManager = createTfsManager();
//		
//		long fileLength = 0;
//		try {
//			fileLength = FileUtility.getFileSize(localFile);
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//			Assert.assertTrue(false);
//		}
//
//		String tfsName = "";
//		if (fileLength > (2 * (1 << 20))) {
//			tfsName = tfsName=tfsManager.saveLargeFile(null,null,data,10,60,key);	
//		} else {
//			tfsName=tfsManager.saveFile(null,null,data,10,60,false);
//		}
//		
//		TimeUtility.sleep(MAX_STAT_TIME);
//		Assert.assertNotNull(tfsName);
//		tfsNames.add(tfsName);
//		log.debug("Saved tfsname is: " + tfsName);	
//		
//		
//		
//		boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
//		Assert.assertTrue(result);
//		//asert localfile md5 and serverfile md5
//		StringBuilder fileMd5after = super.fileMd5("localfile");
//		//IgnoreCase
//		Assert.assertEquals(0,fileMd5after.toString().compareToIgnoreCase(fileMd5before.toString()));
//		tfsManager.destroy();
//
//
//}
//
//
//

}