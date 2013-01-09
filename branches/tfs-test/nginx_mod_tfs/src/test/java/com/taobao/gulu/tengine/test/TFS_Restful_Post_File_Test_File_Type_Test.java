package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Post_File_Test_File_Type_Test extends BaseCase {

	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .bmp
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_01_bmp() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".bmp";
		String localFile = bmpFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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

	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_02_swf() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".swf";
		String localFile = swfFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_03_csw() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".csw";
		String localFile = cswFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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

	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .gif
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_04_gif() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".gif";
		String localFile = gifFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .txt
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_05_txt() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".txt";
		String localFile = txtFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_06_ico() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".ico";
		String localFile = icoFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);

			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .jpg
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_07_jpg() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".jpg";
		String localFile = jpgFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_08_png() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".png";
		String localFile = png10kFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_09_zip() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".zip";
		String localFile = zipFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_10_rar() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".rar";
		String localFile = rarFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithSuffix, suffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
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
	
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未suffix
	 * .bmp
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_11_bmp() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = bmpFile;
		String noSuffix = ".bmp";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		try {

			/* set post method request */
			// setting request info
			String postUrl = url;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.println("tfs file meta without suffix is : " + tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);

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

	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_12_swf() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = swfFile;
		String noSuffix = ".swf";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);

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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_13_csw() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = cswFile;
		String noSuffix = ".csw";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		try {

			/* set post method request */
			// setting request info
			String postUrl = url;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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

	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .gif
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_14_gif() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = gifFile;
		String noSuffix = ".gif";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		
		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .txt
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_15_txt() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = txtFile;
		String noSuffix = ".txt";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		
		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_16_ico() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = icoFile;
		String noSuffix = ".ico";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		
		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);

			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);

			
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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .jpg
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_17_jpg() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = jpgFile;
		String noSuffix = ".jpg";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");


		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_18_png() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = png10kFile;
		String noSuffix = ".png";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");


		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_19_zip() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = zipFile;
		String noSuffix = ".zip";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		
		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta with suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);


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
	
	/* 写文件
	 * 文件测试
	 * 文件类型测试
	 * 未指定suffix
	 * .swf
	 * 写文件成功，返回200。读取该文件判断文件是否完整, meta信息是否正确
	 */
	@Test
	public void test_TFS_Restful_Post_File_Test_File_Type_Test_20_rar() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String localFile = rarFile;
		String noSuffix = ".rar";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");

		
		try {

			/* set post method request */
			// setting request info
			String postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get meta info request */
			// use java client get the meta info
			tfsFileMetaWithOutSuffix = tools.converttfsFileNametoJson(tfsServer, tfsFileNameWithOutSuffix);
			System.out.print("tfs file meta without suffix is : ");
			System.out.println(tfsFileMetaWithOutSuffix);
			// set expect response message
			Map<String, String> expectGetMetaMessageWithOutSuffix = new HashMap<String, String>();
			expectGetMetaMessageWithOutSuffix.put("body", tfsFileMetaWithOutSuffix);
			expectGetMetaMessageWithOutSuffix.put("Content-Type", "application/json");
			expectGetMetaMessageWithOutSuffix.put("status", "200");
			// setting request info -- with suffix
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "?suffix=" + noSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			

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
}
