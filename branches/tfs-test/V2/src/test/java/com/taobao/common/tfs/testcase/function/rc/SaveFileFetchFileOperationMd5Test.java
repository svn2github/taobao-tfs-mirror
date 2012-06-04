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
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class SaveFileFetchFileOperationMd5Test extends BaseCase {


	@Test
	public void testSave1KFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("1K.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave2MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("2M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

	@Test
	public void testSave5MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("5M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}
	@Test
	public void testSave10MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("10M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}
	@Test
	public void testSave20MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("20M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}
	@Test
	public void testSave50MFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("50M.jpg");

		log.info("end: " + getCurrentFunctionName());
	}
	 @Test
		public void testSave1GFile() {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFile("1G.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	// @Test
	public void testSave6GFile() {
		log.info("begin: " + getCurrentFunctionName());

		testSaveFile("6G.jpg");

		log.info("end: " + getCurrentFunctionName());
	}

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
	 @Test
		public void testSaveLargeFileByByteFetchFile20M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("20M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFile100M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("100M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFile1G() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("1G.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	// @Test
		public void testSaveLargeFileByByteFetchFile6G() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFile("6G.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveFileByByteFetchFile1K() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFile("1K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveFileByByteFetchFile100K() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFile("100K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}

	 @Test
		public void testSaveFileByByteFetchFile2M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFile("2M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
//	 @Test
//		public void testSaveFileByByteFetchFile3M() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("3M.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile5M() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("5M.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile10M() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("10M.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile20M() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("20M.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile100M() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("100M.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile1G() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("1G.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
//	 @Test
//		public void testSaveFileByByteFetchFile6G() throws IOException {
//			log.info("begin: " + getCurrentFunctionName());
//
//			testSaveFileByByteFetchFile("6G.jpg");
//
//			log.info("end: " + getCurrentFunctionName());
//		}
	 @Test
		public void testSaveLargeFileByByteFetchFileByByte5M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFileByByte("5M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFileByByte20M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFileByByte("20M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFileByByte100M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFileByByte("100M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveLargeFileByByteFetchFileByByte1G() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFileByByte("1G.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	// @Test
		public void testSaveLargeFileByByteFetchFileByByte6G() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveLargeFileByByteFetchFileByByte("6G.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveFileByByteFetchFileByByte1K() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFileByByte("1K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveFileByByteFetchFileByByte100k() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFileByByte("100K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
	 @Test
		public void testSaveFileByByteFetchFileByByte2M() throws IOException {
			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteFetchFileByByte("2M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte1K() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("1K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte100k() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("100K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte2M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("2M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte5M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("5M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte20M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("20M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFileByByte100M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFileByByte("100M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile1K() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("1K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile100K() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("100K.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile2M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("2M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile5M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("5M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile20M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("20M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}
		
		@Test 
		public void testSaveFileByByteWithOffsetLengthFetchFile100M() throws IOException {

			log.info("begin: " + getCurrentFunctionName());

			testSaveFileByByteWithOffsetLengthFetchFile("100M.jpg");

			log.info("end: " + getCurrentFunctionName());
		}

	private void testSaveFile(String localFile) {

		//get localfile md5
		StringBuilder fileMd5before = super.fileMd5(localFile);
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();

		TimeUtility.sleep(MAX_STAT_TIME);
		long oldUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		long fileLength = 0;
		try {
			fileLength = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}

		String tfsName = "";
		if (fileLength > (2 * (1 << 20))) {
			tfsName = tfsManager.saveLargeFile(localFile, null, null);
		} else {
			tfsName = tfsManager.saveFile(localFile, null, null, false);
		}	
		Assert.assertNotNull(tfsName);
		tfsNames.add(tfsName);
		log.debug("Saved tfsname is: " + tfsName);
		TimeUtility.sleep(MAX_STAT_TIME);
		String sessionId = tfsManager.getSessionId();
		log.debug("sessionId: " + sessionId );
		//Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 2));
		//long newUsedCapacity = tfsStatus.getUsedCapacity(appKey);
		//System.out.println("oldUsedCapacity=" + oldUsedCapacity + "newUsedCapacity=" + newUsedCapacity +"fileLength=" +fileLength );
		//Assert.assertEquals(oldUsedCapacity + fileLength, newUsedCapacity);
		tfsManager = createTfsManager();
		sessionId = tfsManager.getSessionId();
		boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
		//asert localfile md5 and serverfile md5
		StringBuilder fileMd5after = super.fileMd5("localfile");
		Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
		Assert.assertTrue(result);
		TimeUtility.sleep(MAX_STAT_TIME);
		log.debug("expected fileLength is: " + fileLength
				+ "; actual get size is: "
				+ tfsStatus.getFileSize(sessionId, 1));
		log.debug("sessionId: " + sessionId +"tttest");

		//Assert.assertEquals(fileLength, tfsStatus.getFileSize(sessionId, 1));
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

private void testSaveLargeFileByByteFetchFileByByte(String localFile) throws IOException {
	//get byte md5
	byte data[]=null;
	data=getByte(localFile);
	
	String fileMd5before = super.md5(data);
	TfsStatus tfsStatus = new TfsStatus();
	DefaultTfsManager tfsManager = createTfsManager();
	
	
	TimeUtility.sleep(MAX_STAT_TIME);
	String tfsName = "";
	tfsName=tfsManager.saveLargeFile(null,null,data,0,data.length,key);	
	Assert.assertNotNull(tfsName);
	tfsNames.add(tfsName);
	log.debug("Saved tfsname is: " + tfsName);	
	ByteArrayOutputStream output = new ByteArrayOutputStream();
	boolean result = tfsManager.fetchFile(tfsName, null, output);
	log.debug(result+"todaytest");
	Assert.assertTrue(result);	
	//asert localfile md5 and serverfile md5
	String fileMd5after = super.md5(output.toByteArray().clone());
	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
	Assert.assertTrue(result);
	tfsManager.destroy();

}

//saveFile(data, null, null); 5m文件上传失败
private void testSaveFileByByteFetchFile(String localFile) throws IOException {
	//get localfile md5
		StringBuilder fileMd5before = super.fileMd5(localFile);
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();
		byte data[]=null;
		data=getByte(localFile);		
		TimeUtility.sleep(MAX_STAT_TIME);

		String tfsName = "";
		tfsName=tfsManager.saveFile(data, null, null);
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

//saveFile(data, null, null); 5m文件上传失败


private void testSaveFileByByteFetchFileByByte(String localFile) throws IOException {
	//get byte md5
	byte data[]=null;
	data=getByte(localFile);
	String fileMd5before = super.md5(data);
	TfsStatus tfsStatus = new TfsStatus();
	DefaultTfsManager tfsManager = createTfsManager();
	
	TimeUtility.sleep(MAX_STAT_TIME);
	String tfsName = "";
	tfsName=tfsManager.saveFile(data,null,null);	
	Assert.assertNotNull(tfsName);
	tfsNames.add(tfsName);
	log.debug("Saved tfsname is: " + tfsName);	
	ByteArrayOutputStream output = new ByteArrayOutputStream();
	boolean result = tfsManager.fetchFile(tfsName, null, output);
	log.debug(result);
	Assert.assertTrue(result);	
	//assert localbyte md5 and serverbyte md5
	String fileMd5after = super.md5(output.toByteArray().clone());
	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
	Assert.assertTrue(result);
	tfsManager.destroy();

}


private void testSaveFileByByteWithOffsetLengthFetchFileByByte(String localFile) throws IOException {
	//get byte md5
	byte data[]=null;
	byte dataOffset[]=null;
	data=getByte(localFile);
	dataOffset =Arrays.copyOfRange(data,9,69);
	
	String fileMd5before = super.md5(dataOffset);
	TfsStatus tfsStatus = new TfsStatus();
	DefaultTfsManager tfsManager = createTfsManager();
	
	
	long fileLength = 0;
	try {
		fileLength = FileUtility.getFileSize(localFile);
	} catch (FileNotFoundException e) {
		e.printStackTrace();
		Assert.assertTrue(false);
	}

	String tfsName = "";
	if (fileLength > (2 * (1 << 20))) {
		tfsName = tfsName=tfsManager.saveLargeFile(null,null,data,10,60,key);	
	} else {
		tfsName=tfsManager.saveFile(null,null,data,10,60,false);
	}
	
	TimeUtility.sleep(MAX_STAT_TIME);
	Assert.assertNotNull(tfsName);
	tfsNames.add(tfsName);
	log.debug("Saved tfsname is: " + tfsName);	
	ByteArrayOutputStream output = new ByteArrayOutputStream();
	boolean result = tfsManager.fetchFile(tfsName, null, output);
	log.debug(result);
	Assert.assertTrue(result);	
	//assert localbyte md5 and serverbyte md5
	String fileMd5after = super.md5(output.toByteArray().clone());
	System.out.println(fileMd5after);
	Assert.assertEquals(fileMd5before.toString(),fileMd5after.toString());
	Assert.assertTrue(result);
	tfsManager.destroy();

}


private void testSaveFileByByteWithOffsetLengthFetchFile(String localFile) throws IOException {
	//get byte md5
		byte data[]=null;
		byte dataOffset[]=null;
		data=getByte(localFile);
		dataOffset =Arrays.copyOfRange(data,9,69);						
		String fileMd5before = super.md5(dataOffset);
		TfsStatus tfsStatus = new TfsStatus();
		DefaultTfsManager tfsManager = createTfsManager();
		
		long fileLength = 0;
		try {
			fileLength = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}

		String tfsName = "";
		if (fileLength > (2 * (1 << 20))) {
			tfsName = tfsName=tfsManager.saveLargeFile(null,null,data,10,60,key);	
		} else {
			tfsName=tfsManager.saveFile(null,null,data,10,60,false);
		}
		
		TimeUtility.sleep(MAX_STAT_TIME);
		Assert.assertNotNull(tfsName);
		tfsNames.add(tfsName);
		log.debug("Saved tfsname is: " + tfsName);	
		
		
		
		boolean result = tfsManager.fetchFile(tfsName, null, "localfile");
		Assert.assertTrue(result);
		//asert localfile md5 and serverfile md5
		StringBuilder fileMd5after = super.fileMd5("localfile");
		//IgnoreCase
		Assert.assertEquals(0,fileMd5after.toString().compareToIgnoreCase(fileMd5before.toString()));
		tfsManager.destroy();


}




}