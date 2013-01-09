package com.taobao.gulu.tengine.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.unique.UniqueValue;
import com.taobao.gulu.database.TFS;
import com.taobao.gulu.database.Tair;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

/* 写文件
 * 排重
 * 
 */
public class TFS_Restful_Unique_Store_Test extends BaseCase {

	/* init tair */
	Tair tairServer = tair_01;

	@Test
	/*排重接口上传文件a
	 * 不排重接口删除该文件
	 * 重新上传该文件
	 * 成功
	 */
	
	public void test_TFS_Restful_Post_File_Unique_delete_post_agin_2k() 
	{
		
		int filesize = 2 * 1024;
		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithSuffix1 = null;
		String tfsFileNameWithOutSuffix = null;
		String tfsFileNameWithSuffix2 = null;
		/* init tair */
		Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use nginx client */
		String suffix = ".wma";
		String localFile = tmpFile;

		Assert.assertEquals("creat local temp file fail!", 0,tfsServer.createFile(localFile, filesize));

		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");

		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");

		try {

			/* set post method request */
			// setting request info
			tools.verifyCMD(SERVER0435, INIT_DUPLICATE_APP, "", "");
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl with suffix: " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);


			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);

			
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix1 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : "+ tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix1);
			Assert.assertFalse(tfsFileNameWithSuffix1.equals(tfsFileNameWithSuffix));
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
		finally 
		{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) 
			{
				for (String key : put2TfsKeys) 
				{
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			try 
			{
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace(); Assert.assertTrue(false);
			}
			
		}

	}
	@Test
	public void test_TFS_Restful_Post_File_Unique_Same_2k() {
		int filesize = 2 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Same_File_TwiceTest(filesize);

	}

	@Test
	public void test_TFS_Restful_Post_File_Unique_Same_20k() {
		int filesize = 20 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Same_File_TwiceTest(filesize);

	}

	@Test
	public void test_TFS_Restful_Post_File_Unique_Same_2M() {
		int filesize = 2 * 1024 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Same_File_TwiceTest(filesize);

	}

	@Test
	public void test_TFS_Restful_Post_File_Unique_Diff_2k() {
		int filesize = 2 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Diff_File_Test(filesize);

	}

	@Test
	public void test_TFS_Restful_Post_File_Unique_Diff_20k() {
		int filesize = 20 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Diff_File_Test(filesize);

	}

	@Test
	public void test_TFS_Restful_Post_File_Unique_Diff_2M() {
		int filesize = 2 * 1024 * 1024;
		test_TFS_Restful_Post_File_Unique_The_Diff_File_Test(filesize);

	}

	
	
	public void test_TFS_Restful_Post_File_Unique_The_Same_File_TwiceTest(int filesize) 
	{

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;
		String tfsFileNameWithSuffix2 = null;
		/* init tair */
		Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use nginx client */
		String suffix = ".wma";
		String localFile = tmpFile;

		Assert.assertEquals("creat local temp file fail!", 0,tfsServer.createFile(localFile, filesize,true));

		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");

		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			tools.verifyCMD(SERVER0435, INIT_DUPLICATE_APP, "", "");
			
			TimeUnit.SECONDS.sleep(15);
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl with suffix: " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : "+ tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);

			// 再次写文件
			tfsFileNameWithSuffix2 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);

			System.out.println("2ed of the tfs file name with  suffix is  : "+ tfsFileNameWithSuffix2);
			put2TfsKeys.add(tfsFileNameWithSuffix2);
			Assert.assertEquals("post the same file twice but the return filename is diffrent ",tfsFileNameWithSuffix, tfsFileNameWithSuffix2);


			/* set post method request */
			// setting request info
			postUrl = url;
			System.out.println("the postUrl without suffix: " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("of the tfs file name without suffix is  : "+ tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);

			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl),expectGetMessage);

			String retTairTFSName = getTairValueByFile(localFile, filesize);
			System.out.println(retTairTFSName);
			Assert.assertEquals("tair returned file is different from localfile",tfsFileNameWithSuffix + suffix, retTairTFSName);
			// System.out.println(retTairTFSName);

		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
		finally 
		{
			/* do delete tfs file */
			try 
			{
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				
				TimeUnit.SECONDS.sleep(15);
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace(); Assert.assertTrue(false);
			}
			
		}
	}

	
	public void test_TFS_Restful_Post_File_Unique_The_Same_File_Post_Delete_Test(int filesize) 
	{
		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;
		String tfsFileNameWithSuffix2 = null;
		/* init tair */
		Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use nginx client */
		String suffix = ".wma";
		String localFile = tmpFile;

		Assert.assertEquals("creat local temp file fail!", 0,
				tfsServer.createFile(localFile, filesize));

		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");

		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl with suffix: " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(
					setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : "
					+ tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);


			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} finally {
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
		}
	}
	public void test_TFS_Restful_Post_File_Unique_The_Diff_File_Test(int filesize)
	{
		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithSuffix2 = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use nginx client */
		String suffix = ".diff";
		String localFile = tmpFile;
		String localFileDiff = tmpFileDiff;

		Assert.assertEquals("creat local temp file fail!", 0,tfsServer.createFile(localFile, filesize));
		Assert.assertEquals("creat local temp file fail!", 0,tfsServer.createFile(localFileDiff, filesize + 1));
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");

		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);

		Map<String, String> expectPostMessageDiff = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");

		Map<String, String> expectGetMessageDiff = new HashMap<String, String>();
		expectGetMessage.put("body", localFileDiff);
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {
			tools.verifyCMD(SERVER0435, INIT_DUPLICATE_APP, "", "");
			TimeUnit.SECONDS.sleep(15);
			
			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl with suffix: " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : "+ tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);

			//
			tfsFileNameWithSuffix2 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFileDiff),expectPostMessageDiff);

			System.out.println("2ed of the tfs file name with  suffix is  : "+ tfsFileNameWithSuffix2);
			put2TfsKeys.add(tfsFileNameWithSuffix2);
			Assert.assertNotSame("post the diffrent file  but the return filename is same ",tfsFileNameWithSuffix, tfsFileNameWithSuffix2);

			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix="+ suffix;
			System.out.println("the getUrl : " + getUrl);

			/* set post method request */
			// setting request info
			postUrl = url;
			System.out.println("the postUrl without suffix: " + postUrl);

			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the getUrl : " + getUrl);

			//
			String retTairTFSName = getTairValueByFile(localFile, filesize);
			Assert.assertEquals("tair returned file is different from localfile",tfsFileNameWithSuffix + suffix, retTairTFSName);
			String retTairTFSNameDiff = getTairValueByFile(localFileDiff,filesize + 1);
			Assert.assertEquals("tair returned file is different from localfile",tfsFileNameWithSuffix2 + suffix, retTairTFSNameDiff);
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} finally {
			try 
			{
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				
				TimeUnit.SECONDS.sleep(15);
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace(); Assert.assertTrue(false);
			}
		}
	}

	// 根据本地文件生成在tair中的key
	public String getTairValueByFile(String fileForTair, int fileSize)throws Exception
	{

		byte[] fileByte = tairServer.getByte(fileForTair);
		byte[] tairKey = tairServer.getKey(fileByte, 0, fileSize);
		UniqueValue value = tairServer.query(tairKey);
		return value.getTfsName().toString();

	}

}
