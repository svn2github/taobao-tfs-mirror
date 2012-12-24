package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Get_File_Test_File_Type_Test extends BaseCase {


	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .bmp
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_01_bmp() {

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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_02_swf() {

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
			System.out.print("the getUrl with suffix and simple name is : ");
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

	

	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .csw
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_03_csw() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
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
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .gif
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_04_gif() {

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
			
			
			// setting request info -- with simple name and with out suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .txt
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_05_txt() {

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
			
			
			// setting request info -- with simple name and with out suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .ico
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_06_ico() {

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
			
			
			// setting request info -- with simple name and without suffix1
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .jpg
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_07_jpg() {

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
			
			
			// setting request info -- without suffix
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
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .png
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_08_png() {

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
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .zip
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_09_zip() {

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
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .rar
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_10_rar() {

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
			System.out.print("the getUrl with suffix and simple name is : ");
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
	
	/* 读文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .bmp
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */

	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_11_bmp() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".bmp";
		String localFile = bmpFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_12_swf() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".swf";
		String localFile = swfFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix and adding suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .csw
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_13_csw() {

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
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			// setting request info -- without suffix
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);

			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .gif
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_14_gif() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".gif";
		String localFile = gifFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .txt
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_15_txt() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".txt";
		String localFile = txtFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);

			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .ico
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_16_ico() {

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
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .jpg
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_17_jpg() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".jpg";
		String localFile = jpgFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .png
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_18_png() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".png";
		String localFile = png10kFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and with out suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .zip
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_19_zip() {

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
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with out suffix and with simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .rar
	 * 读文件成功，返回200。验证该文件是否完整及content-type与Accept-Ranges 头设置
	 */


	@Test
	public void test_TFS_Restful_Get_File_Test_File_Type_Test_20_rar() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();

		/* put file into tfs use java client */
		// put file with suffix
		String suffix = ".rar";
		String localFile = rarFile;
		String tfsFileNameWithSuffix = tfsServer.put(localFile);
		System.out.println("the tfs file name with suffix is : " + tfsFileNameWithSuffix + "(java client)");
		// put file with suffix and simpleName
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
		//  tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			
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
	//		tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			
			// setting request info -- with simple name and without suffix
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName;
			System.out.print("the getUrl with suffix and simple name is : ");
			System.out.println(getUrl);
			
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
	//		tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			// setting request info -- with simple name and without suffix  and adding  suffix in filename
			getUrl = url + "/" + tfsFileNameWithSuffixAndSimpleName + suffix;
			System.out.print("the getUrl with suffix and without simple name is : ");
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
}
