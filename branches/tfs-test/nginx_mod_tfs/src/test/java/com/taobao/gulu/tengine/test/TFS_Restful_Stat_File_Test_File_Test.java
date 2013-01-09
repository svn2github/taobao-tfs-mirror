package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Stat_File_Test_File_Test extends BaseCase 
{

	/*
	 * 
	 * 公共方法 得到文件元信息,largefile 文件测试 文件大小测试 文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	public void test_TFS_Restful_Stat_LargeFile_Size_Test(long filesize) 
	{
		if (filesize > 2 * 1024 * 1024) 
		{
			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA01;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".tmp";
			String localFile = tmpFile;
			String type = "1";
			Assert.assertEquals("creat local temp file fail!", 0, tfsServer.createFile(localFile, filesize));
			String tfsFileNameWithSuffix = null;
			tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
			
			System.out.println("the tfs file name with suffix is : "+ tfsFileNameWithSuffix + "(java client)");
			put2TfsKeys.add(tfsFileNameWithSuffix);

			/* get file meta info use java client */
			String tfsFileMetaWithSuffix = "";
			try 
			{
				tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
				System.out.print("tfs file meta with suffix is : ");
				System.out.println(tfsFileMetaWithSuffix);
			} 
			catch (Exception e1) 
			{
				e1.printStackTrace();
			}

			/* set expect response message */
			Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
			expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMessageWithSuffix.put("status", "200");

			try 
			{
				/* set get method request */
				// setting request info -- with suffix
				String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffix);

				// setting request info -- without suffix
				getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffix);

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
				deleteFile(localFile);
			}
		} 
		else 
		{
			System.out.println("This file is not large file");
		}
	}

	/*
	 * 
	 * 公共方法 得到文件元信息 小文件 文件测试 文件大小测试 文件(.tmp)
	 * stat件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	public void test_TFS_Restful_Stat_File_Size(long filesize) 
	{
		if (filesize <= 2 * 1024 * 1024 && filesize > 0) 
		{
			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA01;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".tmp";
			String localFile = tmpFile;
			String type = "1";
			Assert.assertEquals("creat local temp file fail!", 0, tfsServer.createFile(localFile, filesize));
			String tfsFileNameWithSuffix = null;
			String tfsFileNameWithSuffixAndSimpleName = null;

			tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
			tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
			put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);

			System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
			put2TfsKeys.add(tfsFileNameWithSuffix);

			// put file with suffix and simpleName
			System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");

			/* get file meta info use java client */
			String tfsFileMetaWithSuffix = "";
			String tfsFileMetaWithSuffixAndSimpleName = "";

			try 
			{
				tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
				tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer,tfsFileNameWithSuffixAndSimpleName, suffix);

				System.out.print("tfs file meta with suffix is : ");
				System.out.println(tfsFileMetaWithSuffix);
				System.out.print("tfs file meta with suffix and simple name is : ");
				System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			}

			catch (Exception e1) 
			{
				e1.printStackTrace();
			}

			/* set expect response message */
			Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
			expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMessageWithSuffix.put("status", "200");

			Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
			expectGetMessageWithSuffixAndSimpleName.put("body",tfsFileMetaWithSuffixAndSimpleName);
			expectGetMessageWithSuffixAndSimpleName.put("Content-Type","application/json");
			expectGetMessageWithSuffix.put("status", "200");

			Map<String, String> expectErrorMessage = new HashMap<String, String>();
			expectErrorMessage.put("status", "500");

			try 
			{
				/* set get method request */
				// setting request info -- with suffix
				String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffix);

				// setting request info -- without suffix
				getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffix);

				// setting request info -- with suffix and simple name
				getMetaUrl = url + "/metadata/"+ tfsFileNameWithSuffixAndSimpleName + "?suffix="+ suffix;
				System.out.print("the getUrl with suffix and simple name is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffixAndSimpleName);

				// setting request info -- with simple name and without suffix
				getMetaUrl = url + "/metadata/"+ tfsFileNameWithSuffixAndSimpleName;
				System.out.print("the getUrl with suffix and without simple name is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				// tools.verifyResponse(setGetMethod(getMetaUrl),
				// expectErrorMessage);
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),expectGetMessageWithSuffixAndSimpleName);

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
				deleteFile(localFile);
			}
		} 
		else 
		{
			System.out.println("This file is not regular small  file");
		}
	}

	/*
	 * 
	 * 得到文件元信息 文件测试 文件大小测试 100K文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_01_1K() {

		long filesize = 1 * 1024;
		test_TFS_Restful_Stat_File_Size(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_02_100K() {

		long filesize = 100 * 1024;
		test_TFS_Restful_Stat_File_Size(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_03_1m() {

		long filesize = 1 * 1024 * 1024;
		test_TFS_Restful_Stat_File_Size(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_04_2m() {

		long filesize = 2 * 1024 * 1024;
		test_TFS_Restful_Stat_File_Size(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_05_10m() {

		long filesize = 10 * 1024 * 1024;
		test_TFS_Restful_Stat_LargeFile_Size_Test(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_06_30m() {

		long filesize = 30 * 1024 * 1024;
		test_TFS_Restful_Stat_LargeFile_Size_Test(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_06_400m() {

		long filesize = 400 * 1024 * 1024;
		test_TFS_Restful_Stat_LargeFile_Size_Test(filesize);
	}

	@Test
	public void test_TFS_Restful_Stat_File_Test_File_Size_06_300m() {

		long filesize = 300 * 1024 * 1024;
		test_TFS_Restful_Stat_LargeFile_Size_Test(filesize);
	}

	/*
	 * 
	 * 得到文件元信息 文件测试 文件大小测试 2M 文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */
}
