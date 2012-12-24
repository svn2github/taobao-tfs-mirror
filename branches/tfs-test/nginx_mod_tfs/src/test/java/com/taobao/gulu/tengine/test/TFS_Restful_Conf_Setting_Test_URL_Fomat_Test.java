package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_Conf_Setting_Test_URL_Fomat_Test extends BaseCase {

	/* 配置设置
	 * url请求格式测试
	 * 访问路径为V1
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_01_V1() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		String special_url = NGINX.getRoot_url_adress();
		special_url = special_url + "V1/" + tfsServer.getTfs_app_key();
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = special_url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get method request */
			// setting request info
			getUrl = special_url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 访问路径为v01
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_02_v01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		String special_url = NGINX.getRoot_url_adress();
		special_url = special_url + "v01/" + tfsServer.getTfs_app_key();
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = special_url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get method request */
			// setting request info
			getUrl = special_url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 访问路径为v10
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_03_v10() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		String special_url = NGINX.getRoot_url_adress();
		special_url = special_url + "v10/" + tfsServer.getTfs_app_key();
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = special_url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get method request */
			// setting request info
			getUrl = special_url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 访问路径为v11
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_04_v11() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		String special_url = NGINX.getRoot_url_adress();
		special_url = special_url + "v11/" + tfsServer.getTfs_app_key();
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = special_url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get method request */
			// setting request info
			getUrl = special_url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
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


	/* 配置设置
	 * url请求格式测试
	 * 格式 GET /v1/appkey?suffix=.jpg&suffix=.jpg HTTP/1.1,suffix重复且相同
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_05_suffix_repeat_same() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 GET /v1/appkey?suffix=.jpg&suffix=.bmp HTTP/1.1,suffix重复不相同
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_06_suffix_repeat_diff_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String fsuffix = ".kk";
		String localFile = _2kFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&suffix=" + fsuffix + "&suffix=" + fsuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 GET /v1/appkey?suffix=.jpg&suffix=.bmp HTTP/1.1,suffix重复不相同
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_06_suffix_repeat_diff_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String fsuffix = ".kk";
		String localFile = _2kFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + fsuffix + "&suffix=" + suffix + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 错误格式 GET /v1/appkey/Filename/ HTTP/1.1，Filename后面多/
	 * 返回404， 将fileName后面的/，当作后缀的一部分进行处理了
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_07_filename() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		
		Map<String, String> expectGetErrorMessage400 = new HashMap<String, String>();
		expectGetErrorMessage400.put("status", "400");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "/?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage400);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			put2TfsKeys.add(tfsFileNameWithOutSuffix);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix + "/" ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 设置 GET /v1/appkey/filename?suffix=.jpg&size=xx HTTP/1.1，文件存在，互换size与suffix的位置
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_08_suffix_size_exchange_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int size = 20;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&size=" + size;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, size);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 设置 GET /v1/appkey/filename?suffix=.jpg&size=xx HTTP/1.1，文件存在，互换size与suffix的位置
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_08_suffix_size_exchange_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int size = 20;
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

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
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?size=" + size + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, size);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 GET /v1/appkey/filename?suffix HTTP/1.1,suffix且=空
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_09_suffix_empty() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int size = 20;
		String simple_name  = "1";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size=" + size + "&suffix=";
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 GET /v1/appkey/filename?suffix HTTP/1.1,suffix且为空
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_10_suffix_empty() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int size = 20;
		String simple_name = "1";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size=" + size + "&suffix";
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 /v1/appkey?suffix=.jpg&&size=x HTTP/1.1，&&分隔
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_11_symbol() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int size = 20;
		String simple_name = "1";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "404");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size=" + size + "&&&&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage, 0, size);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 DELETE /v1/appkey?suffix=.jpg&size HTTP/1.1，size无值
	 * 返回200 
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_12_size_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		String simple_name = "1";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size" + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 DELETE /v1/appkey?suffix=.jpg&size HTTP/1.1，size=无值
	 * 返回400  
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_12_size_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		String simple_name = "1";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size=" + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
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
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 DELETE /v1/appkey?suffix=.jpg&size HTTP/1.1，size=字符
	 * 返回200 , size 为0
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_12_size_03() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		String simple_name = "1";
		String size = "size";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix + "&simple_name=" + simple_name;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			put2TfsKeys.add(tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + splitString(tfsFileNameWithSuffix) + "?size=" + size + "&suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponse(setGetMethod(getUrl), expectGetErrorMessage);
			
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 格式 DELETE /v1/appkey/filename?suffix=.jpg&type=字符 HTTP/1.1，type=字符t
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_13_type() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int expectStatu = 0;
		String type = "t";
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectDeleteErrorMessage = new HashMap<String, String>();
		expectDeleteErrorMessage.put("status", "400");
		

		try {

			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteErrorMessage);
			
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithOutSuffix + "?type=" + type;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteErrorMessage);
			
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithOutSuffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/* 配置设置
	 * url请求格式测试
	 * 访问路径为多个“/”
	 * 返回400
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_14_() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;
		String tfsFileMetaWithSuffix = null;
		String tfsFileMetaWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		
		String special_url = NGINX.getRoot_url_adress();
		special_url = special_url + "v1///" + tfsServer.getTfs_app_key();
		
		/* set expect response message */
		Map<String, String> expectPostMessage = new HashMap<String, String>();
		expectPostMessage.put("Content-Type", "application/json");
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("body", localFile);
		/* ---------------------------------------------- */
		//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */
		
		Map<String, String> expectDeleteMessage = new HashMap<String, String>();
		expectDeleteMessage.put("status", "200");
		
		Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
		expectGetErrorMessage.put("status", "400");
		

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
			String getMetaUrl = special_url + "///////////metadata///////////////////" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = special_url + "/////////////////////" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
//			tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
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
			getMetaUrl = url + "/metadata/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl without suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithOutSuffix);
			
			
			/* set get method request */
			// setting request info
			getUrl = special_url + "//////////////" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
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
	
	
	/* 配置设置
	 * url请求格式测试
	 * 访问路径为的变量为大写
	 * 返回200
	 */
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_15_args() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileMetaWithSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA03;
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
			String getMetaUrl = url + "/metadata/" + tfsFileNameWithSuffix + "?SUffix=" + suffix;
			System.out.println("the getUrl with suffix is : " + getMetaUrl);
			/* do get file action */
			//tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseWithJSON(setGetMethod(getMetaUrl), expectGetMetaMessageWithSuffix);
			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?SUFFIX=" + suffix;
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
	
	
	@Test
	public void test_TFS_Restful_Conf_Setting_Test_URL_Fomat_Test_16_args() throws Exception {
		VerifyTool tools = new VerifyTool();
		tools.verifyCMD(SERVER036209, "/home/gongyuan.cz/a.sh", "", "");
	}
	
	
}
