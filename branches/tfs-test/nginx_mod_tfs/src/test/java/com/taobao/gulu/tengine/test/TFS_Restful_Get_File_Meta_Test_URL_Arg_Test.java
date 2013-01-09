package com.taobao.gulu.tengine.test;

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Get_File_Meta_Test_URL_Arg_Test extends BaseCase {

	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  Appkey测试
	 *  appkey不存在且filename存在
	 *  返回500
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_01_Appkey() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1" ;

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
		expectErrorMessage.put("status", "500");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getMetaUrl));
	//		tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
		//	tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
		//	tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getMetaUrl));
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Appkey测试,6个Appkey
	 *  appkey存在且正确
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_02_Appkey() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer1 = tfs_NginxA01;
		TFS tfsServer2 = tfs_NginxA02;
		TFS tfsServer3 = tfs_NginxA03;
		TFS tfsServer4 = tfs_NginxA01;
		TFS tfsServer5 = tfs_NginxA02;
		TFS tfsServer6 = tfs_NginxA03;
		
		String url = NGINX.getRoot_url_adress();
	//	url = url + "v1/" ;
		String url1 = url + "v1/" + tfsServer1.getTfs_app_key();
		String url2 = url + "v1/" + tfsServer2.getTfs_app_key();
		String url3 = url + "v1/" + tfsServer3.getTfs_app_key();
		String url4 = url + "v1/" + tfsServer4.getTfs_app_key();
		String url5 = url + "v1/" + tfsServer5.getTfs_app_key();
		String url6 = url + "v1/" + tfsServer6.getTfs_app_key();
		
		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithSuffix1 = tfsServer1.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix1 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix1);
		
		String tfsFileNameWithSuffix2 = tfsServer2.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix2 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix2);
		
		String tfsFileNameWithSuffix3 = tfsServer3.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix3 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix3);
		
		String tfsFileNameWithSuffix4 = tfsServer4.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix4 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix4);
		
		String tfsFileNameWithSuffix5 = tfsServer5.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix5 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix5);
		
		String tfsFileNameWithSuffix6 = tfsServer6.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix6 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix6);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName1 = tfsServer1.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName1 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName1);
		
		String tfsFileNameWithSuffixAndSimpleName2 = tfsServer2.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName2 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName2);
		
		String tfsFileNameWithSuffixAndSimpleName3 = tfsServer3.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName3 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName3);
		
		String tfsFileNameWithSuffixAndSimpleName4 = tfsServer4.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName4 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName4);
		
		String tfsFileNameWithSuffixAndSimpleName5 = tfsServer5.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName5 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName5);
		
		String tfsFileNameWithSuffixAndSimpleName6 = tfsServer6.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName6 + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName6);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix1 = "";
		String tfsFileMetaWithSuffixAndSimpleName1 = "";
		String tfsFileMetaWithSuffix2 = "";
		String tfsFileMetaWithSuffixAndSimpleName2 = "";
		String tfsFileMetaWithSuffix3 = "";
		String tfsFileMetaWithSuffixAndSimpleName3 = "";
		String tfsFileMetaWithSuffix4 = "";
		String tfsFileMetaWithSuffixAndSimpleName4 = "";
		String tfsFileMetaWithSuffix5 = "";
		String tfsFileMetaWithSuffixAndSimpleName5 = "";
		String tfsFileMetaWithSuffix6 = "";
		String tfsFileMetaWithSuffixAndSimpleName6 = "";
		
		try {
			tfsFileMetaWithSuffix1 = tools.converttfsFileNametoJson(tfsServer1, tfsFileNameWithSuffix1, suffix);
			tfsFileMetaWithSuffixAndSimpleName1 = tools.converttfsFileNametoJson(tfsServer1, tfsFileNameWithSuffixAndSimpleName1, suffix);
			
			tfsFileMetaWithSuffix2 = tools.converttfsFileNametoJson(tfsServer2, tfsFileNameWithSuffix2, suffix);
			tfsFileMetaWithSuffixAndSimpleName2 = tools.converttfsFileNametoJson(tfsServer2, tfsFileNameWithSuffixAndSimpleName2, suffix);
			
			tfsFileMetaWithSuffix3 = tools.converttfsFileNametoJson(tfsServer3, tfsFileNameWithSuffix3, suffix);
			tfsFileMetaWithSuffixAndSimpleName3 = tools.converttfsFileNametoJson(tfsServer3, tfsFileNameWithSuffixAndSimpleName3, suffix);
			
			tfsFileMetaWithSuffix4 = tools.converttfsFileNametoJson(tfsServer4, tfsFileNameWithSuffix4, suffix);
			tfsFileMetaWithSuffixAndSimpleName4 = tools.converttfsFileNametoJson(tfsServer4, tfsFileNameWithSuffixAndSimpleName4, suffix);
			
			tfsFileMetaWithSuffix5 = tools.converttfsFileNametoJson(tfsServer5, tfsFileNameWithSuffix5, suffix);
			tfsFileMetaWithSuffixAndSimpleName5 = tools.converttfsFileNametoJson(tfsServer5, tfsFileNameWithSuffixAndSimpleName5, suffix);
			
			tfsFileMetaWithSuffix6 = tools.converttfsFileNametoJson(tfsServer6, tfsFileNameWithSuffix6, suffix);
			tfsFileMetaWithSuffixAndSimpleName6 = tools.converttfsFileNametoJson(tfsServer6, tfsFileNameWithSuffixAndSimpleName6, suffix);
			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix1);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName1);
		
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix2);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName2);
		
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix3);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName3);
		
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix4);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName4);
		
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix5);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName5);
		
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix6);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName6);
		
		/* set expect response message */
		Map<String, String> expectGetMessageWithSuffix1 = new HashMap<String, String>();
		expectGetMessageWithSuffix1.put("body", tfsFileMetaWithSuffix1);
		expectGetMessageWithSuffix1.put("Content-Type", "application/json");
		expectGetMessageWithSuffix1.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName1 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName1.put("body", tfsFileMetaWithSuffixAndSimpleName1);
		expectGetMessageWithSuffixAndSimpleName1.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName1.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffix2 = new HashMap<String, String>();
		expectGetMessageWithSuffix2.put("body", tfsFileMetaWithSuffix2);
		expectGetMessageWithSuffix2.put("Content-Type", "application/json");
		expectGetMessageWithSuffix2.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName2 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName2.put("body", tfsFileMetaWithSuffixAndSimpleName2);
		expectGetMessageWithSuffixAndSimpleName2.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName2.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffix3 = new HashMap<String, String>();
		expectGetMessageWithSuffix3.put("body", tfsFileMetaWithSuffix3);
		expectGetMessageWithSuffix3.put("Content-Type", "application/json");
		expectGetMessageWithSuffix3.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName3 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName3.put("body", tfsFileMetaWithSuffixAndSimpleName3);
		expectGetMessageWithSuffixAndSimpleName3.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName3.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffix4 = new HashMap<String, String>();
		expectGetMessageWithSuffix4.put("body", tfsFileMetaWithSuffix4);
		expectGetMessageWithSuffix4.put("Content-Type", "application/json");
		expectGetMessageWithSuffix4.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName4 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName4.put("body", tfsFileMetaWithSuffixAndSimpleName4);
		expectGetMessageWithSuffixAndSimpleName4.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName4.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffix5 = new HashMap<String, String>();
		expectGetMessageWithSuffix5.put("body", tfsFileMetaWithSuffix5);
		expectGetMessageWithSuffix5.put("Content-Type", "application/json");
		expectGetMessageWithSuffix5.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName5 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName5.put("body", tfsFileMetaWithSuffixAndSimpleName5);
		expectGetMessageWithSuffixAndSimpleName5.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName5.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffix6 = new HashMap<String, String>();
		expectGetMessageWithSuffix6.put("body", tfsFileMetaWithSuffix6);
		expectGetMessageWithSuffix6.put("Content-Type", "application/json");
		expectGetMessageWithSuffix6.put("status", "200");
		
		Map<String, String> expectGetMessageWithSuffixAndSimpleName6 = new HashMap<String, String>();
		expectGetMessageWithSuffixAndSimpleName6.put("body", tfsFileMetaWithSuffixAndSimpleName6);
		expectGetMessageWithSuffixAndSimpleName6.put("Content-Type", "application/json");
		expectGetMessageWithSuffixAndSimpleName6.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl1 = url1 + "/metadata/" + tfsFileNameWithSuffix1 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl1);
			
			String getMetaUrl2 = url2 + "/metadata/" + tfsFileNameWithSuffix2 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl2);
			
			String getMetaUrl3 = url3 + "/metadata/" + tfsFileNameWithSuffix3 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl3);
			
			String getMetaUrl4 = url4 + "/metadata/" + tfsFileNameWithSuffix4 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl4);
			
			String getMetaUrl5 = url5 + "/metadata/" + tfsFileNameWithSuffix5 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl5);
			
			String getMetaUrl6 = url6 + "/metadata/" + tfsFileNameWithSuffix6 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl6);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl1));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl1), expectGetMessageWithSuffix1);
		//	tools.showResponse(setGetMethod(getMetaUrl2));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl2), expectGetMessageWithSuffix2);
		//	tools.showResponse(setGetMethod(getMetaUrl3));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl3), expectGetMessageWithSuffix3);
		//	tools.showResponse(setGetMethod(getMetaUrl4));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl4), expectGetMessageWithSuffix4);
		//	tools.showResponse(setGetMethod(getMetaUrl5));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl5), expectGetMessageWithSuffix5);
		//	tools.showResponse(setGetMethod(getMetaUrl6));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl6), expectGetMessageWithSuffix6);
		//	tools.verifyResponse(setGetMethod(getMetaUrl1), expectErrorMessage);
			
			
			// setting request info -- without suffix
			getMetaUrl1 = url1 + "/metadata/" + tfsFileNameWithSuffix1;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl1);
			getMetaUrl2 = url2 + "/metadata/" + tfsFileNameWithSuffix2;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl2);
			getMetaUrl3 = url3 + "/metadata/" + tfsFileNameWithSuffix3;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl3);
			getMetaUrl4 = url4 + "/metadata/" + tfsFileNameWithSuffix4;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl4);
			getMetaUrl5 = url5 + "/metadata/" + tfsFileNameWithSuffix5;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl5);
			getMetaUrl6 = url6 + "/metadata/" + tfsFileNameWithSuffix6;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl6);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl1), expectGetMessageWithSuffix1);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl2), expectGetMessageWithSuffix2);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl3), expectGetMessageWithSuffix3);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl4), expectGetMessageWithSuffix4);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl5), expectGetMessageWithSuffix5);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl6), expectGetMessageWithSuffix6);
		//	tools.verifyResponse(setGetMethod(getMetaUrl1), expectErrorMessage);
			
			// setting request info -- with suffix and simple name
			getMetaUrl1 = url1 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName1 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl1);
			
			getMetaUrl2 = url2 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName2 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl2);
			
			getMetaUrl3 = url3 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName3 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl3);
			
			getMetaUrl4 = url4 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName4 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl4);
			
			getMetaUrl5 = url5 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName5 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl5);
			
			getMetaUrl6 = url6 + "/metadata/" + tfsFileNameWithSuffixAndSimpleName6 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl6);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
		//	tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl1), expectGetMessageWithSuffixAndSimpleName1);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl2), expectGetMessageWithSuffixAndSimpleName2);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl3), expectGetMessageWithSuffixAndSimpleName3);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl4), expectGetMessageWithSuffixAndSimpleName4);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl5), expectGetMessageWithSuffixAndSimpleName5);
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl6), expectGetMessageWithSuffixAndSimpleName6);
			
//			// setting request info -- with simple name and without suffix
//			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName;
//			System.out.print("the getUrl with suffix and without simple name is : ");
//			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
//			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
			/* do delete tfs file */
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix1);
			tfsServer1.delete(tfsFileNameWithSuffix1, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix2);
			tfsServer2.delete(tfsFileNameWithSuffix2, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix3);
			tfsServer3.delete(tfsFileNameWithSuffix3, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix4);
			tfsServer4.delete(tfsFileNameWithSuffix4, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix5);
			tfsServer5.delete(tfsFileNameWithSuffix5, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix6);
			tfsServer6.delete(tfsFileNameWithSuffix6, null);
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName1);
			tfsServer1.delete(tfsFileNameWithSuffixAndSimpleName1, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName2);
			tfsServer2.delete(tfsFileNameWithSuffixAndSimpleName2, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName3);
			tfsServer3.delete(tfsFileNameWithSuffixAndSimpleName3, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName4);
			tfsServer4.delete(tfsFileNameWithSuffixAndSimpleName4, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName5);
			tfsServer5.delete(tfsFileNameWithSuffixAndSimpleName5, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName6);
			tfsServer6.delete(tfsFileNameWithSuffixAndSimpleName6, null);
			put2TfsKeys.clear();
		}
	}
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确,名字超过18位
	 *  只取了前18位，余下的全部当成后缀，
	 *  错误提示400 OR 404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_01_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "Unexist" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "Unexist" + "?suffix=" + "Unexist";
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "Unexist" ;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "Unexist" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			// setting request info -- with simple name and without suffix
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName+ "Unexist" + "?suffix=" + ".cswUnexist";;
			System.out.print("the getUrl with suffix and without simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确
	 *  名字不足18位
	 *  返回error 400 bad request
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_02_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "TunexistFilename" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			getMetaUrl = url + "/metadata/" + "TunexistFilename" ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确，18位的不存在的filename Tnexistfilename018
	 *  返回error 404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_03_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "TnexistFilename018" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			
			getMetaUrl = url + "/metadata/" + "TnexistFilename018" ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确，18位的不存在的filename Tnexistfilename018.jpg
	 *  返回error 404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_04_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "TnexistFilename018.jpg" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			
			getMetaUrl = url + "/metadata/" + "TnexistFilename018.jpg" ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage404);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确，18位的不存在的filename Unexistfilename018
	 *  没有以T开头
	 *  返回error 400
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_05_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "Unexistfilename018" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			getMetaUrl = url + "/metadata/" + "Unexistfilename018" ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在但不正确，18位的不存在的filename Unexistfilename018.jpg
	 *  没有以T开头
	 *  返回error 400
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_06_filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "Unexistfilename018.jpg" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			getMetaUrl = url + "/metadata/" + "Unexistfilename018.jpg" ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename存在且正确
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_07_filename() {

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
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  FileName测试
	 *  filename不存在
	 *  返回400
	 * 
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_08_filename() {

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
		
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			
			
			// setting request info -- without suffix
			getMetaUrl = url + "/metadata/" ;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage400);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为bmp但未设置suffix，获取元信息文件的suffix为bmp
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_01_Suffix_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为bmp但未设置suffix，获取元信息文件的suffix为swf
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_02_Suffix_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String localFile = swfFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为csw但未设置suffix，获取元信息文件的suffix为csw
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_03_Suffix_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = cswFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为gif但未设置suffix，获取元信息文件的suffix为gif
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_04_Suffix_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".gif";
		String localFile = gifFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为txt但未设置suffix，获取元信息文件的suffix为txt
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_05_Suffix_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".txt";
		String localFile = txtFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为ico但未设置suffix，获取元信息文件的suffix为ico
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_06_Suffix_ico() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".ico";
		String localFile = icoFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为jpg但未设置suffix，获取元信息文件的suffix为jpg
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_07_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String localFile = jpgFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为png但未设置suffix，获取元信息文件的suffix为png
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_08_Suffix_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".png";
		String localFile = pngFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为txt但未设置suffix，获取元信息文件的suffix为zip
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_09_Suffix_zip() throws InterruptedException {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".zip";
		String localFile = zipFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
			/* do delete tfs file */
			TimeUnit.SECONDS.sleep(20);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与所写文件类型不一致（未设置suffix）
	 *  所写文件为rar但未设置suffix，获取元信息文件的suffix为rar
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_10_Suffix_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".rar";
		String localFile = rarFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file with suffix and simpleName
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* get file meta info use java client */
		String tfsFileMetaWithSuffix = "";
		String tfsFileMetaWithSuffixAndSimpleName = "";
		try {
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件bmp，suffix为bmpp
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_11_Suffix_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String getsuffix = ".bmpp";
		
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件swf，suffix为swf.
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_12_Suffix_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String getsuffix = ".swf.";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件csw，suffix为swf
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_13_Suffix_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String getsuffix = ".swf";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件gif，suffix为*if
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_14_Suffix_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".gif";
		String getsuffix = ".*if";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件txt，suffix为t*t
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_15_Suffix_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".txt";
		String getsuffix = ".t*t";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件ico，suffix为ic
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_16_Suffix_ico() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".ico";
		String getsuffix = ".ic";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件jpg，suffix为jpeg
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_17_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpeg";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件png，suffix为png*
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_18_Suffix_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".png";
		String getsuffix = ".png*";
		
		String localFile = pngFile;
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件zip，suffix为rar
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_19_Suffix_zip() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".zip";
		String getsuffix = ".rar";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  Suffix测试
	 *  suffix与写文件时所设置suffix类型不一致
	 *  所写文件rar，suffix为zip
	 *  返回404
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_20_Suffix_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".rar";
		String getsuffix = ".zip";
		
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
		expectErrorMessage.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getMetaUrl = url + "/metadata/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  metadata测试
	 *  metadata不存在
	 *  读文件成功，返回200。
	 *  验证该文件是否完整及content-type与Accept-Ranges 头设置
	 *  
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_01_metadata() {

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
		
	
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url  + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/"  + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  metadata测试
	 *  metadata存在
	 *  获取文件元信息成功，返回200。
	 *  验证该文件所有原信息是否正确（file name，block id，file id，offset，size，occupy size，modify_time， create_time， statu， crc）
	 *  
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_02_metadata() {

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
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为0
	 *  要读的文件被删除
	 *  无法获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_01_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			TimeUnit.SECONDS.sleep(20);
			
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after delete is : ");
			System.out.println(getMetaUrl);
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after delete is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
//			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  type测试
	 *  值为1
	 *  要读的文件被删除
	 *  正常获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_02_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="1";
		int expectStatu = 1;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, expectStatu);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			System.out.print("tfs file meta with suffix after delete is : ");
			System.out.println(tfsFileNameWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAfterDelete = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAfterDelete.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffixAfterDelete.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAfterDelete.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAfterDelete);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
			System.out.print("tfs file meta with suffix and simple name after delete is : ");
			System.out.println(tfsFileNameWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAndSimpleNameAfterDelete = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAndSimpleNameAfterDelete.put("body", tfsFileMetaWithSuffixAndSimpleName);
			expectGetMetaMessageWithSuffixAndSimpleNameAfterDelete.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAndSimpleNameAfterDelete.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAndSimpleNameAfterDelete);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
//			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  type测试
	 *  值未设置
	 *  默认为0
	 *  要读的文件被删除
	 *  无法获取该文件
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_03_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
	//	String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after delete is : ");
			System.out.println(getMetaUrl);
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.println("the deleteUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after delete is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
//			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  type测试
	 *  值为0
	 *  要读的文件被隐藏
	 *  无法获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_04_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu_hide = 4;
		String type_hide = "4";
		String type_delete = "0";
		String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterHide = new HashMap<String, String>();
		expectGetMessageAfterHide.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu_hide);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after hide is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after hide */
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu_hide);
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterHide);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为1
	 *  要读的文件被隐藏
	 *  正常获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_05_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu_hide = 4;
		String type_hide = "4";
		String type_delete = "0";
		String type="1";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
	
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu_hide);
		
			
			
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after hide is : ");
			System.out.println(getMetaUrl);
			//do get file action 
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			System.out.print("tfs file meta with suffix after delete is : ");
			System.out.println(tfsFileNameWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAfterHide = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAfterHide.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffixAfterHide.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAfterHide.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
			System.out.print("tfs file meta with suffix after delete is : ");
			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAndSimpleNameAfterHide = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("body", tfsFileMetaWithSuffixAndSimpleName);
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAndSimpleNameAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值未设置
	 *  默认为0
	 *  要读的文件被隐藏
	 *  无法获取该文件
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_06_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu_hide = 4;
		String type_hide = "4";
		String type_delete = "0";
//		String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterHide = new HashMap<String, String>();
		expectGetMessageAfterHide.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu_hide);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after hide is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after hide */
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + deleteUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu_hide);
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterHide);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为0
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_07_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="0";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为1
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_08_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="1";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值未设置
	 *  默认type=0
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_09_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
	//	String type="0";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为-2
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_10_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="-2";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		/* set expect response message */
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为01
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_11_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="01";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		/* set expect response message */
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为10
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_12_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="10";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		/* set expect response message */
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为00
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_13_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="00";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		/* set expect response message */
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为11
	 *  要读的文件正常存在
	 *  可以获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_14_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String type="11";
		String localFile = bmpFile;
		int expectStatu = 0;
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		System.out.print("tfs file meta with suffix is : ");
		System.out.println(tfsFileMetaWithSuffix);
		System.out.print("tfs file meta with suffix and simple name is : ");
		System.out.println(tfsFileMetaWithSuffixAndSimpleName);
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		/* set expect response message */
		
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
		
			
		
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectErrorMessage);
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
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
	 *  url参数测试
	 *  type测试
	 *  值为0
	 *  要读的文件先被隐藏后被删除
	 *  无法获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_15_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu = 5;
		String type_hide = "4";
		String type_delete = "0";
		String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterHideAndDelete = new HashMap<String, String>();
		expectGetMessageAfterHideAndDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectHideMessage = new HashMap<String, String>();
		expectHideMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set hide method request */
			// setting request info to hide the tfs file
			String hideUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(hideUrl), expectHideMessage);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after hide and delete is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after hide and delete */
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterHideAndDelete);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			hideUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(hideUrl), expectDeleteMessage);
			/* set delete method request */
			// setting request info to hide the tfs file
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide and delete is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterHideAndDelete);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  type测试
	 *  值为1
	 *  要读的文件先被隐藏后被删除
	 *  可以获取该文件
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_16_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu = 5;
		String type_hide = "4";
		String type_delete = "0";
		String type="1";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterHideAndDelete = new HashMap<String, String>();
		expectGetMessageAfterHideAndDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectHideMessage = new HashMap<String, String>();
		expectHideMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set hide method request */
			// setting request info to hide the tfs file
			String hideUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(hideUrl), expectHideMessage);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getMetaUrl with suffix after hide and delete is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
			System.out.print("tfs file meta with suffix after delete is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAfterHide = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAfterHide.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffixAfterHide.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAfterHide.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getMetaUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			hideUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(hideUrl), expectDeleteMessage);
			/* set delete method request */
			// setting request info to hide the tfs file
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide and delete is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after delete */
			// use java client get the meta info
			tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName);
			System.out.print("tfs file meta with suffix after delete is : ");
			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffixAndSimpleNameAfterHide = new HashMap<String, String>();
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("body", tfsFileMetaWithSuffixAndSimpleName);
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffixAndSimpleNameAfterHide.put("status", "200");
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffixAndSimpleNameAfterHide);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
	/*
	 * 
	 *  得到文件元信息
	 *  url参数测试
	 *  type测试
	 *  值未设置
	 *  默认为0
	 *  要读的文件先被隐藏后被删除
	 *  无法获取该文件元信息
	 *  
	 */
	@Test
	public void test_TFS_Restful_Get_File_Meta_Test_URL_Arg_Test_17_Type() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		int expectStatu = 5;
		String type_hide = "4";
		String type_delete = "0";
//		String type="0";
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
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix);
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
		
		Map<String, String> expectGetMessageAfterHideAndDelete = new HashMap<String, String>();
		expectGetMessageAfterHideAndDelete.put("status", "404");
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectHideMessage = new HashMap<String, String>();
		expectHideMessage.put("status", "200");
		
		try {
			/* set get meta method request */
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
		//	String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffix);
			
			/* set hide method request */
			// setting request info to hide the tfs file
			String hideUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(hideUrl), expectHideMessage);
			
			/* set delete method request */
			// setting request info to hide the tfs file
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			/* set get meta method request */
			// setting request info -- with suffix
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix after hide and delete is : ");
			System.out.println(getMetaUrl);
			
			/* set get meta info request after hide and delete */
			//do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageAfterHideAndDelete);
		//	tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterDelete);
			
			
			/* set get meta method request */
			// setting request info -- with suffix 
			getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getMetaUrl);
			//do get file action before delete
//			tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMessageWithSuffixAndSimpleName);
			/* set hide method request */
			// setting request info
			hideUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_hide;
			System.out.println("the hideUrl : " + hideUrl);
			// do delete file aciton 
			tools.verifyResponse(setDeleteMethod(hideUrl), expectDeleteMessage);
			/* set delete method request */
			// setting request info to hide the tfs file
			deleteUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type_delete;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); TimeUnit.SECONDS.sleep(20);
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix, expectStatu);
			// setting meta request info -- with suffix and simple name
	//		getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&type=" + type;
			System.out.print("the getUrl with suffix and simple name after hide and delete is : ");
			System.out.println(getMetaUrl);
			// do get file action 
		//	tools.showResponse(setGetMethod(getMetaUrl));
			tools.verifyResponse(setGetMethod(getMetaUrl), expectGetMessageAfterHideAndDelete);
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}finally{
			/* do delete tfs file */
//			if (put2TfsKeys.size() > 0) {
//				for (String key : put2TfsKeys) {
//					System.out.println("tfsFileName for delete is " + key);
//					tfsServer.delete(key, null);
//				}
//			}
//			put2TfsKeys.clear();
		}
	}
	
}
