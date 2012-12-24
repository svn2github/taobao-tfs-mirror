package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Get_File_Test_URL_Arg_Test extends BaseCase {


	/* 读文件
	 * url参数测试
	 * Appkey测试
	 * appkey不存在且filename存在
	 * 返回400
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_Appkey() throws InterruptedException {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		//url = url + "v1/" + tfsServer.getTfs_app_key();
		url = url + "v1" ;
		
		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			
//			// setting request info -- with simple name and without suffix
//			String getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
//			System.out.print("the getUrl with suffix and simple name is : ");
//			System.out.println(getUrl);
//			
//			/* do get file action */
//			//tools.showResponse(setGetMethod(getUrl));
//			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
//			
//			String tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, suffix);
//			System.out.print("the getUrl without suffix and with simple name is : ");
//			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			TimeUnit.SECONDS.sleep(20);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix);
			tfsServer.delete(tfsFileNameWithSuffix, null);
		}

	}
	
	/* 
	 * 读文件
	 * url参数测试
	 * Appkey测试,6个Appkey
	 * appkey存在且正确
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_Appkey() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer1 = tfs_NginxA01;
		TFS tfsServer2 = tfs_NginxA02;
		TFS tfsServer3 = tfs_NginxA03;
		
		String url = NGINX.getRoot_url_adress();
		String url1 = url + "v1/" + tfsServer1.getTfs_app_key();
		String url2 = url + "v1/" + tfsServer2.getTfs_app_key();
		String url3 = url + "v1/" + tfsServer3.getTfs_app_key();


		/* put file into tfs use java client */
		// put file with suffix ;appkey = tfs_NginxA01
		String suffix = ".swf";
		String localFile = _XXXX14MFile;
		String tfsFileNameWithSuffix1 = tfsServer1.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix1 + "(java client)");
		// put file with suffix and simpleName ;appkey = tfs_NginxA01
		String tfsFileNameWithSuffixAndSimpleName1 = tfsServer1.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName1 + "(java client)");
		
		// put file with suffix ;appkey = tfs_NginxA02
		String tfsFileNameWithSuffix2 = tfsServer2.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix2 + "(java client)");
		// put file with suffix and simpleName ;appkey = tfs_NginxA02
		String tfsFileNameWithSuffixAndSimpleName2 = tfsServer2.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName2 + "(java client)");
		
		// put file with suffix ;appkey = tfs_NginxA03
		String tfsFileNameWithSuffix3 = tfsServer3.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix3 + "(java client)");
		// put file with suffix and simpleName ;appkey = tfs_NginxA03
		String tfsFileNameWithSuffixAndSimpleName3 = tfsServer3.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName3 + "(java client)");
		
	
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
			String getUrl1 = url1 + "/" + tfsFileNameWithSuffix1 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl1);
			
			String getUrl2 = url2 + "/" + tfsFileNameWithSuffix2 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl2);
			
			String getUrl3 = url3 + "/" + tfsFileNameWithSuffix3 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl3);
			
		
		
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl1), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl2), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl3), expectGetMessage);
			
			// setting request info -- with out suffix
			getUrl1 = url1 + "/" + tfsFileNameWithSuffix1;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl1);
			
			getUrl2 = url2 + "/" + tfsFileNameWithSuffix2;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl2);
			
			getUrl3 = url3 + "/" + tfsFileNameWithSuffix3;
			System.out.print("the getUrl with out suffix is : ");
			System.out.println(getUrl3);
			

			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl1), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl2), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl3), expectGetMessage);

			
			// setting request info -- with suffix and simple name
			getUrl1 = url1 + "/" + tfsFileNameWithSuffixAndSimpleName1 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl1);
			
			getUrl2 = url2 + "/" + tfsFileNameWithSuffixAndSimpleName2 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl2);
			
			getUrl3 = url3 + "/" + tfsFileNameWithSuffixAndSimpleName3 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl3);
			
			
			getUrl1 = url1 + "/" + tfsFileNameWithSuffixAndSimpleName1 + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl1);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl1), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl2), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl3), expectGetMessage);
			
			
			// setting request info -- with simple name and without suffix1
			getUrl1 = url1 + "/" + tfsFileNameWithSuffixAndSimpleName1;
			System.out.print("the getUrl without suffix and with simple name is :  ");
			System.out.println(getUrl1);
			
			getUrl2 = url2 + "/" + tfsFileNameWithSuffixAndSimpleName2;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl2);
			
			getUrl3 = url3 + "/" + tfsFileNameWithSuffixAndSimpleName3;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl3);
			

			
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl1));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl1), expectGetMessage);
			tools.showResponse(setGetMethod(getUrl2));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl2), expectGetMessage);
			tools.showResponse(setGetMethod(getUrl3));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl3), expectGetMessage);

			
			
			// setting request info -- with simple name and without suffix2
			getUrl1 = url1 + "/" + splitString(tfsFileNameWithSuffixAndSimpleName1);
			System.out.print("the getUrl with simple name and without suffix is : ");
			System.out.println(getUrl1);
			
			getUrl2 = url2 + "/" + splitString(tfsFileNameWithSuffixAndSimpleName2);
			System.out.print("the getUrl with simple name and without suffix is : ");
			System.out.println(getUrl2);
			
			getUrl3 = url3 + "/" + splitString(tfsFileNameWithSuffixAndSimpleName3);
			System.out.print("the getUrl with simple name and without suffix is : ");
			System.out.println(getUrl3);
			
		
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl1), expectErrorMessage);
			tools.verifyResponse(setGetMethod(getUrl2), expectErrorMessage);
			tools.verifyResponse(setGetMethod(getUrl3), expectErrorMessage);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix1);
			tfsServer1.delete(tfsFileNameWithSuffix1, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix2);
			tfsServer2.delete(tfsFileNameWithSuffix2, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix3);
			tfsServer3.delete(tfsFileNameWithSuffix3, null);


			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName1);
			tfsServer1.delete(tfsFileNameWithSuffixAndSimpleName1, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName2);
			tfsServer2.delete(tfsFileNameWithSuffixAndSimpleName2, null);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName3);
			tfsServer3.delete(tfsFileNameWithSuffixAndSimpleName3, null);

		}

	}

	

	/* 读文件
	 * url参数测试
	 * FileName测试
	 * filename不存在,名字超过18位
	 * 只取了前18位，余下的全部当成后缀，
	 * 错误提示400 OR 404
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_Filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = _XXXX14MFile;
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
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "Unexist" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "Unexist" + "?suffix=" + "Unexist";
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
					
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			// setting request info -- without suffix
			getUrl = url + "/" + tfsFileNameWithSuffix+ "Unexist";
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "Unexist" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "Unexist" + "?suffix=" + ".cswUnexist";
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
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix);
			tfsServer.delete(tfsFileNameWithSuffix, null);
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	/* 读文件
	 * url参数测试
	 * FileName测试
	 * filename存在但不正确，不是以T开头
	 * 名字不足18位
	 * 返回error 400 bad request
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_Filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = _XXXX14MFile;
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
		Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
		expectErrorMessage400.put("status", "400");
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix filename length is 15
			String getUrl = url + "/" +  "TunexistFilename" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
			
			
			// setting request info -- without suffix
			getUrl = url + "/" + "TunexistFilename";
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
			
			
//			// setting request info -- with suffix and simple name
//			getUrl = url + "/" +  "UnexistFilename" + "?suffix=" + suffix;
//			System.out.print("the getUrl with suffix and simple name is : ");
//			System.out.println(getUrl);
//			/* do get file action */
//			//tools.showResponse(setGetMethod(getUrl));
//			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
//			

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
	
	
	
	/* 读文件
	 * url参数测试
	 * FileName测试
	 * filename存在但不正确，18位的不存在的filename Tnexistfilename018
	 * 返回error 404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_03_Filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String localFile = _XXXX14MFile;
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
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" +  "TnexistFilename018" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			
			// setting request info -- without suffix
			getUrl = url + "/" + "TnexistFilename018";
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			

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
	
	/* 读文件
	 * url参数测试
	 * FileName测试
	 * filename存在但不正确,18位的不存在的filename Txxxxxx.jpg
	 * 返回error 404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_04_Filename() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String localFile = _XXXX14MFile;
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
		Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
		expectErrorMessage404.put("status", "404");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" +  "TnexistFilename018.jpg" + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			
			// setting request info -- without suffix
			getUrl = url + "/" + "TnexistFilename018.jpg";
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
			
			

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
		

		/* 读文件
		 * url参数测试
		 * FileName测试
		 * filename存在但不正确,18位的不存在的filename Uxxxxxx...
		 * 没有以T开头
		 * 返回error 400
		 */


		@Test
		public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_05_Filename() {

			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA02;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".csw";
			String localFile = _XXXX14MFile;
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
			Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
			expectErrorMessage400.put("status", "400");
			
			try {
				/* set get method request */
				// setting request info -- with suffix
				String getUrl = url + "/" +  "UnexistFilename018" + "?suffix=" + suffix;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
			//	tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
				
				// setting request info -- without suffix
				getUrl = url + "/" + "UnexistFilename018";
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
		//		tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
				

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
		
		/* 读文件
		 * url参数测试
		 * FileName测试
		 * filename存在但不正确,18位的不存在的filename Uxxxxxx.jpg
		 * 没有以T开头
		 * 返回error 400
		 */


		@Test
		public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_06_Filename() {

			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA02;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".jpg";
			String localFile = _XXXX14MFile;
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
			Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
			expectErrorMessage400.put("status", "400");
			
			try {
				/* set get method request */
				// setting request info -- with suffix
				String getUrl = url + "/" +  "UnexistFilename018.jpg" + "?suffix=" + suffix;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
			//	tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
				
				// setting request info -- without suffix
				getUrl = url + "/" + "UnexistFilename018.jpg";
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
		//		tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
				

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
		
		/* 读文件
		 * url参数测试
		 * FileName测试
		 * filename存在且正确
		 * 读文件成功，返回200。验证该文件是否完整
		 * 
		 */


		@Test
		public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_07_Filename() {

			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA02;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".csw";
			String localFile = _XXXX14MFile;
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
			Map<String, String> expectErrorMessage404 = new HashMap<String, String>();
			expectErrorMessage404.put("status", "404");
			
			
			try {
				/* set get method request */
				// setting request info -- with suffix
				String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
			//	tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
				
			
				
				// setting request info -- without suffix
				getUrl = url + "/" + tfsFileNameWithSuffix;
				System.out.print("the getUrl without suffix is : ");
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
				
				// setting request info -- with suffix and simple name
				getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName ;
				System.out.print("the getUrl with suffix and simple name is : ");
				System.out.println(getUrl);
				/* do get file action */
				//tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			//	tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
				// setting request info -- with suffix and simple name
				getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) ;
				System.out.print("the getUrl with suffix and simple name is : ");
				System.out.println(getUrl);
				/* do get file action */
				//tools.showResponse(setGetMethod(getUrl));
			//	tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage404);
				
			
				

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
		

		/* 读文件
		 * url参数测试
		 * FileName测试
		 * filename 不存在
		 * 返回400。
		 * 
		 */


		@Test
		public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_08_Filename() {

			VerifyTool tools = new VerifyTool();

			/* set base url */
			TFS tfsServer = tfs_NginxA02;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();

			/* put file into tfs use java client */
			// put file with suffix
			String suffix = ".csw";
			String localFile = _XXXX14MFile;
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
			Map<String, String> expectErrorMessage400 = new HashMap<String, String>();
			expectErrorMessage400.put("status", "400");
			
			
			try {
				/* set get method request */
				// setting request info -- with suffix
				String getUrl = url + "/" + "?suffix=" + suffix;
				System.out.print("the getUrl with suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
			//	tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);
				
			
				
				// setting request info -- without suffix
				getUrl = url ;
				System.out.print("the getUrl without suffix is : ");
				System.out.println(getUrl);
				
				/* do get file action */
				//tools.showResponse(setGetMethod(getUrl));
				tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage400);

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
		
		
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为bmp但未设置suffix，读文件的suffix为bmp
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_Suffix_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为swf但未设置suffix，读文件的suffix为swf
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_Suffix_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".swf";
		String localFile = _XXXX14MFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为csw但未设置suffix，读文件的suffix为csw
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_03_Suffix_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		String localFile = _XXXX14MFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为gif但未设置suffix，读文件的suffix为gif
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_04_Suffix_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".gif";
		String localFile = gifFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为txt但未设置suffix，读文件的suffix为txt
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_05_Suffix_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".txt";
		String localFile = txtFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为ico但未设置suffix，读文件的suffix为ico
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_06_Suffix_ico() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".ico";
		String localFile = icoFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为jpg但未设置suffix，读文件的suffix为jpg
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_07_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".jpg";
		String localFile = _XXXX14MFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为png但未设置suffix，读文件的suffix为png
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_08_Suffix_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".png";
		String localFile = pngFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为zip但未设置suffix，读文件的suffix为zip
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_09_Suffix_zip() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".zip";
		String localFile = zipFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与所写文件类型不一致
	 * 所写文件为rar但未设置suffix，读文件的suffix为rar
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_10_Suffix_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".rar";
		String localFile = rarFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
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
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为bmp，suffix为bmpp
	 * 返回404
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_11_Suffix_bmp() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为swf，suffix为swf.
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_12_Suffix_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String getsuffix = ".swf.";
		String localFile = _XXXX14MFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
		
			
			String tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, getsuffix);
			System.out.print("tfs file meta with suffix and simple name is : ");
			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			
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
	

	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为csw，suffix为swf
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_13_Suffix_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".csw";
		String getsuffix = ".swf";
		String localFile = _XXXX14MFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
		
			
			String tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, getsuffix);
			System.out.print("tfs file meta with suffix and simple name is : ");
			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为gif，suffix为*if
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_14_Suffix_gif() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
		
			
			String tfsFileMetaWithSuffixAndSimpleName = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffixAndSimpleName, getsuffix);
			System.out.print("tfs file meta with suffix and simple name is : ");
			System.out.println(tfsFileMetaWithSuffixAndSimpleName);
			
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为txt，suffix为t*t
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_15_Suffix_txt() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为ico，suffix为ic
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_16_Suffix_ico() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为jpg，suffix为jpeg
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_17_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpeg";
		String localFile = _XXXX14MFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为png，suffix为png*
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_18_Suffix_png() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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

	
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为zip，suffix为rar
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_19_Suffix_zip() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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
	
	/* 读文件
	 * url参数测试
	 * Suffix测试
	 * suffix与写文件时所设置suffix类型不一致
	 * 所写文件为rar，suffix为zip
	 * 返回404
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_20_Suffix_rar() {

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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件的文件(xxxx.bmp),但读取是文件名为xxxx，指定suffix后缀.bmp
	 * 从文件名中不能解析出该suffix ，以指定suffix的值为准作为suffix,找到这个文件xxxx.bmp
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_21_Suffix_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String getsuffix = ".bmp";
		String localFile = bmpFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			//tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
//			
//			// setting request info -- without suffix
//			getUrl = url + "/" + tfsFileNameWithSuffix;
//			System.out.print("the getUrl without suffix is : ");
//			System.out.println(getUrl);
//			
//			/* do get file action */
//			//tools.showResponse(setGetMethod(getUrl));
//			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
//			// setting request info -- with simple name and without suffix
//			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName);
//			System.out.print("the getUrl with suffix and simple name is : ");
//			System.out.println(getUrl);
//			
			/* do get file action */
//			//tools.showResponse(setGetMethod(getUrl));
//			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
//			
	
			
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件的文件(xxxx.bmp),但读取是文件名为xxxx.jpg，指定suffix后缀.bmp
	 * 返回400 bad request
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_22_Suffix_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String filesuffix = ".gif";
		String getsuffix = ".bmp";
		String localFile = bmpFile;
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
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + filesuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
		
			
			getUrl = url + "/" + tfsFileNameWithSuffix + getsuffix + "?suffix=" + filesuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);

			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + filesuffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
	
			
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件的文件名xxxx.jpg，没有指定suffix后缀
	 * 可以从文件名中解析出该suffix ，找到这个文件
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_23_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpg";
		String localFile = _XXXX14MFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + getsuffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
	
			
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件的文件名xxxx.jpg，指定suffix后缀.jpg
	 * 可以从文件名中解析出该suffix ，以指定suffix的值为准作为suffix,找到这个文件xxxx.jpg
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_24_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpg";
		String localFile = _XXXX14MFile;
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + getsuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
	
			
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件的文件名xxxx.jpg，指定suffix后缀.jpeg
	 * 返回400 bad request
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_25_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpeg";
		String localFile = _XXXX14MFile;
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
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + suffix + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
		//	tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
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
	 * url参数测试
	 * Suffix测试
	 * 读取一个存在的文件xxxx.jpg,读取时的文件名为xxxx.jpeg，指定suffix后缀.jpg
	 * 返回400 bad request
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_26_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpeg";
		String localFile = _XXXX14MFile;
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
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + getsuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + getsuffix + "?suffix=" + suffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			//tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
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
	 * url参数测试
	 * Suffix测试
	 * java客户端存第二种文件（xxxx.jpg）
	 * nginx客户端读取xxxx（无.jpg）, 没有指定suffix后缀
	 * 返回404
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_27_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpg";
		String localFile = _XXXX14MFile;
		
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
		
			/* do get file action */
			// setting request info -- filename xxxxx with no suffix
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
	
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	/* 
	 * 读文件
	 * url参数测试
	 * Suffix测试
	 * java客户端存第二种文件（xxxx.jpg）
	 * nginx客户端读取xxxx（无.jpg）, 指定suffix后缀 .jpeg
	 * 返回404
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_28_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpeg";
		String localFile = _XXXX14MFile;
		
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
		
			/* do get file action */
			// setting request info -- filename xxxxx with no suffix
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
		//	tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage );
	
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	/* 
	 * 读文件
	 * url参数测试
	 * Suffix测试
	 * java客户端存第二种文件（xxxx.jpg）
	 * nginx客户端读取xxxx（无.jpg）, 指定suffix后缀 .jpg
	 * 读文件成功，返回200。验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_29_Suffix_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String getsuffix = ".jpg";
		String localFile = _XXXX14MFile;
		
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
		
			/* do get file action */
			// setting request info -- filename xxxxx with no suffix
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffixAndSimpleName) + "?suffix=" + getsuffix;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			//tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage );
	
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	
	/* 读文件
	 * url参数测试
	 * Size测试
	 * size小于要读文件大小
	 * 读文件成功，返回200。
	 * 验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int readsize1= 1;
		int readsize100= 100;
		int readsize1024= 1024;
		int readsize10241023= 1024*1023;
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
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
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1 ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize1);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize100;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize100);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1024;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize1024);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize10241023;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize10241023);
			
			
	
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize1);
			
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize100;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize100);
			
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1024;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize1024);
			
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize10241023;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, readsize10241023);
			
			

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
	
	/* 读文件
	 * url参数测试
	 * Size测试
	 * size大于要读文件大小
	 * 读文件成功，返回200。
	 * 验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 1024;
		String readsize1025= "1025";
		String readsize10240= "10240";
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
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
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1025 ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize10240;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1025;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize10240;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			

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
	
	/* 读文件
	 * url参数测试
	 * Size测试
	 * size远远大于要读文件大小
	 * 读文件成功，返回200。
	 * 验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_03_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 1024;
		String readsize1025= "10250000";
		String readsize1024= "102400000000000";
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
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
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1025 ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1024;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
		
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1025;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1024;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			

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
	
	/* 读文件
	 * url参数测试
	 * Size测试
	 * size 等于要读文件大小
	 * 读文件成功，返回200。
	 * 验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_04_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 1024;
		String readsize1024= "1024";
		
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
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
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" +readsize1024 ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
		
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&size=" +readsize1024;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			
			
			

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
	
	/* 读文件
	 * url参数测试
	 * Size测试
	 * Size未设置
	 * 读文件成功，返回200。
	 * 验证该文件是否符合所要求的size大小及返回内容是否正确，验证content-type与Accept-Ranges 头设置
	 * 
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_05_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 1024;
		String readsize1024= "1024";
		
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
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
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
		
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, realsize);
			
			
			
			

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
	 *  读文件
	 * url参数测试
	 * Size测试
	 * Size为-1
	 * 返回400。
	 *
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_06_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 1024;
		String readsize= "-1";
		
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" + readsize;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
		
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix  + "&size=" + readsize;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
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
			

		
		}
	}
	

	/*
	 *  读文件
	 * url参数测试
	 * Size测试
	 * Size为0
	 * 返回400。
	 *
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_07_Size() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".csw";
		int realsize = 0;
		String readsize= "0";
		
		
		String localFile = _XXXX14MFile;
		Assert.assertEquals("creat local temp file fail!" , 0, tfsServer.createFile(localFile, 1 * 1024));
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffix);
		
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, suffix, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		put2TfsKeys.add(tfsFileNameWithSuffixAndSimpleName);
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info 
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" + readsize;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectErrorMessage, 0, realsize);
			
		
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix  + "&size=" + readsize;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectErrorMessage, 0, realsize);
		//	tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Offset测试
	 * 值为0
	 * 要读取数据在文件中的偏移为0，读整个文件
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_Offset() throws InterruptedException {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String offset = "0";
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* do delete tfs file */
			TimeUnit.SECONDS.sleep(20);
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffix);
			tfsServer.delete(tfsFileNameWithSuffix, null);
			
			System.out.println("tfsFileName for delete is " + tfsFileNameWithSuffixAndSimpleName);
			tfsServer.delete(tfsFileNameWithSuffixAndSimpleName, null);
		}

	}
	
	/* 读文件
	 * url参数测试
	 * Offset测试
	 * 值为-1
	 * 返回Bad Request
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_Offset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String offset = "-1";
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile, true);
		System.out.println("the tfs file name with suffix and simple name is : " + tfsFileNameWithSuffixAndSimpleName + "(java client)");
		
		/* set expect response message */
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		expectGetMessage.put("status", "200");
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset;
			System.out.print("the getUrl without suffix and with simple name is : ");
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
	
	/* 读文件
	 * url参数测试
	 * Offset测试
	 * 值为某个小于文件偏移的值
	 * 读取从偏移开始的文件数据
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_03_Offset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String offset = "1";
		int offset_int = 1;
		String tfsFileNameWithSuffix = tfsServer.put(localFile ,suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile ,suffix ,true);
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);

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
	
	/* 读文件
	 * url参数测试
	 * Offset测试
	 * 值为某个大于文件大小的偏移值
	 * 返回Bad Request400
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_04_Offset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String offset = "177285400000000";
	//	String offset = "1";
		
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
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
		expectErrorMessage.put("status", "400");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset;
	//		String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset;
			System.out.print("the getUrl without suffix and with simple name is : ");
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
	

	/* 读文件
	 * url参数测试
	 * Offset测试
	 * Offset未设置
	 *读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_05_Offset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
	//	String offset = "0";
		String tfsFileNameWithSuffix = tfsServer.put(localFile, suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix ;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

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
	
	/* 读文件
	 * url参数测试
	 * Offset测试
	 * 值为等于文件大小的偏移的值
	 * 读取文件数据为0
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_06_Offset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String offset = "1772854";
		int offset_int = 1772854;
		String tfsFileNameWithSuffix = tfsServer.put(localFile ,suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile ,suffix ,true);
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
		Map<String, String> expectMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "200");
		
		try {
			/* set get method request */
			// setting request info -- with suffix
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
		
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int);

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
	 * url参数测试
	 * Size和Offset 组合测试 
	 * （假设文件大小10，offset=7,size=5）
	 * 实际文件大小1772854,offset=1772851,size=10
	 * 读取文件数据为3
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_01_SizeOffset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
//		String realsize = "1772854";
		String size = "10";
		String offset = "1772851";
		int offset_int = 1772851;
		int exp_size = 3;
		String tfsFileNameWithSuffix = tfsServer.put(localFile ,suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile ,suffix ,true);
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);

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
	 * url参数测试
	 * Size和Offset 组合测试 
	 * （假设文件大小10，offset=7,size=1）
	 * 实际文件大小1772854,offset=1772851,size=1
	 * 读取文件数据为1
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_02_SizeOffset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
//		String realsize = "1772854";
		String size = "1";
		String offset = "1772851";
		int offset_int = 1772851;
		int exp_size = 1;
		String tfsFileNameWithSuffix = tfsServer.put(localFile ,suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile ,suffix ,true);
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);

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
	 * url参数测试
	 * Size和Offset 组合测试 
	 * （假设文件大小10，offset=10,size=1）
	 * 实际文件大小1772854,offset=1772854,size=5
	 * 读取文件数据为0
	 * 
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_URL_Arg_Test_03_SizeOffset() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file without suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
//		String realsize = "1772854";
		String size = "5";
		String offset = "1772854";
		int offset_int = 1772854;
		int exp_size = 0;
		String tfsFileNameWithSuffix = tfsServer.put(localFile ,suffix);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file  with simpleName and without suffix
		String tfsFileNameWithSuffixAndSimpleName = tfsServer.put(localFile ,suffix ,true);
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
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
		//	tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffix + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			
			// setting request info -- with suffix and simple name
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?suffix=" + suffix + "&offset=" + offset + "&size=" + size;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + "?offset=" + offset + "&size=" + size;
			System.out.print("the getUrl without suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, offset_int, exp_size);

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
}
