package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Get_File_Meta_Test_File_Type_Test extends BaseCase {

	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  bmp
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_01_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName);
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

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
		}
	}
	
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  swf
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_02_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String localFile = swfFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

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
		}
	}

	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  csw
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_03_csw() {

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
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  gif
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_04_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".gif";
		String localFile = gifFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  txt
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_05_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".txt";
		String localFile = txtFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  ico
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_06_ico() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".ico";
		String localFile = icoFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  jpg
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_07_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String localFile = jpgFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  png
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_08_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".png";
		String localFile = png10kFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  zip
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_09_zip() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".zip";
		String localFile = zipFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  指定suffix
	 *  rar
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_10_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".rar";
		String localFile = rarFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  bmp
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_11_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  swf
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_12_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String localFile = swfFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  csw
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_13_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = cswFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  gif
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_14_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".gif";
		String localFile = gifFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  txt
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_15_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".txt";
		String localFile = txtFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  ico
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_16_ico() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".ico";
		String localFile = icoFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  jpg
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_17_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String localFile = jpgFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  png
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_18_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".png";
		String localFile = png10kFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  zip
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_19_zip() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".zip";
		String localFile = zipFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  文件测试
	 *  文件类型测试
	 *  没有指定suffix
	 *  rar
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_File_Type_Test_20_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".rar";
		String localFile = rarFile;
		String tfsFileNameWithOutSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with out suffix is : " + tfsFileNameWithOutSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithOutSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with out suffix and with simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix = new HashMap<String, String>();
		expectGetMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
		expectGetMessageWithSuffix.put("Content-Type", "application/json");
		expectGetMessageWithSuffix.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName.put("body", tfsFileMetaWithSuffixAndSimpleName);
		expectGetMessageWithSuffixAndSimpleName.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

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
		}
	}
	
}
