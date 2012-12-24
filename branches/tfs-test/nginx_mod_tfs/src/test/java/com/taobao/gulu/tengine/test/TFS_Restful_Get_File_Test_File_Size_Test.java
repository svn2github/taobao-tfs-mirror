package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Get_File_Test_File_Size_Test extends BaseCase {
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 1K文件(.CSW)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_01_Size_1K() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = cswFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix2
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName);
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix);
			tfsServer.delete(tfsFileNameWithSuffix, null);
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 300K文件(.wav)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_01_Size_300K() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".wav";
		String localFile = wavFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix2
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName);
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix);
			tfsServer.delete(tfsFileNameWithSuffix, null);
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 1M文件(.wma)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_01_Size_1M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1*1024 * 1024));
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix2
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName);
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println("hellodiqing");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 10M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_01_Size_10M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 10*1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}

	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 2M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_01_Size_2M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 2 * 1024 * 1024));
		
		

		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		/* set expect response message */
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}

	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 3M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_02_Size_3M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 3 * 1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		/* set expect response message */
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			System.out.println("hello1");
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 10M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_03_Size_10M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 10 * 1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		/* set expect response message */
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 20M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_04_Size_20M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 20 * 1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 30M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_05_Size_30M_() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 30 * 1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		/* set expect response message */
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	
	/* 
	 * 读文件
	 * 文件测试
	 * 文件大小测试
	 * 100M文件(.tmp)
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Test_File_Size_Test_06_Size_100M() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		String suffix = ".tmp";
		String localFile = tmpFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 100 * 1024 * 1024));
		
		// put file with suffix
		String tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file without suffix
		String tfsFileNameWithOutSuffix = tfsServer.putLarge(localFile);
		System.out.println("the tfs file name without suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		//expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* set expect response message */
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		/* set expect response message */
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "404");
		try {
			/* set get method request */
			// setting request info -- tfsFileNameWithSuffix and request with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- tfsFileNameWithOutSuffix and request with suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- tfsFileNameWithOutSuffix and request with out suffix
			getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
			
			/* do delete file */
			deleteFile(localFile);
		}

	}
	

	/*
	 * 
	 * 公共方法 get文件, 文件测试 文件大小测试 文件(.tmp)
	 * get文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	
	
	
	
	
	public void test_TFS_Restful_Get_File_Size_400M_Test( ) {
		int filesize=400*1024*1024;
		test_TFS_Restful_Get_LargeFile_Size_Test(filesize);
	}
	public void test_TFS_Restful_Get_File_Size_401M_Test( ) {
		int filesize=500*1024*1024;
		test_TFS_Restful_Get_LargeFile_Size_Test(filesize);
	}
	
	//fun???
	public void test_TFS_Restful_Get_LargeFile_Size_Test(long filesize) {
		if (filesize > 2 * 1024*1024) {
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
			Assert.assertEquals("creat local temp file fail!", 0,
					tfsServer.createFile(localFile, filesize));
			String tfsFileNameWithSuffix = null;
			tfsFileNameWithSuffix = tfsServer.putLarge(localFile, suffix);
			System.out.println("the tfs file name with suffix is : "
					+ tfsFileNameWithSuffix + "(java client)");
			put2TfsKeys.add(tfsFileNameWithSuffix);

			/* get file meta info use java client */
			String tfsFileMetaWithSuffix = "";
			try {
				tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(
						tfsServer, tfsFileNameWithSuffix, suffix);
				System.out.print("tfs file meta with suffix is : ");
				System.out.println(tfsFileMetaWithSuffix);
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			/* set expect response message */
			Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
			expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMessageWithSuffix.put("status", "200");

			try {
				/* set get method request */
				// setting request info -- with suffix
				String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix
						+ "?suffix=" + suffix;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),
						expectGetMessageWithSuffix);

				// setting request info -- without suffix
				getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getMetaUrl);

				/* do get file action */
				// tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseWithJSON(setGetMethod(getMetaUrl),
						expectGetMessageWithSuffix);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				/* do delete tfs file */
				if (put2TfsKeys.size() > 0) {
					for (String key : put2TfsKeys) {
						System.out.println("tfsFileName for delete is " + key);
						tfsServer.delete(key, null);
					}
				}
				put2TfsKeys.clear();
				/* do delete file */
				deleteFile(localFile);
			}
		} else {
			System.out.println("This file is not large file");
		}
	}

}
