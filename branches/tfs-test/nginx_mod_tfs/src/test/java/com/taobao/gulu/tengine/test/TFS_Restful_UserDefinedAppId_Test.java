package com.taobao.gulu.tengine.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.unique.UniqueValue;
import com.taobao.gulu.database.TFS;
import com.taobao.gulu.database.Tair;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

/* 自定义文件名测试
 * 
 * 
 */
public class TFS_Restful_UserDefinedAppId_Test extends BaseCase {
	/*
	 * 
	 *  
	 *  根据appkey获得appid 
	 *  存在的appkey 
	 *  返回appid=1
	 *  
	 */
	@Test
	public void test_TFS_RestfulUserDefinedGetAppidByExistAppkeyTest() {

		VerifyTool tools = new VerifyTool();
		/* init tair */
		//Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v2/" + tfsServer.getTfs_app_key()+"/appid";
		System.out.println(url);


		/* set expect response message */
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("Content-Type", "application/json");
		expectGetMessage.put("body", "\"APP_ID\""+":"+" \"1\"");
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set get method request */
			// setting request info
			String getUrl = url;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			//InputStream  getMethod =setGetMethod(getUrl).getResponseBodyAsStream();
			
			//System.out.println(getMethod.toString());
			tools.verifyResponse(setGetMethod(getUrl),expectGetMessage);
			

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
		}
	}

	
	/*
	 * 
	 *  
	 *  根据appkey获得appid 
	 *  不存在的appkey appkey=NonExistAppKey
	 *  返回 500
	 *  
	 */
	@Test
	public void test_TFS_RestfulUserDefinedGetAppidByNonExistAppkeyTest() {

		VerifyTool tools = new VerifyTool();
		/* init tair */
		//Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String nonExistAppKey = "NonExistAppKey";
		url = url + "v2/" + nonExistAppKey+"/appid";
		System.out.println(url);


		/* set expect response message */
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("status", "500");
		//expectGetMessage.put("body", "\"APP_ID\""+":"+" \"1\"");
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set get method request */
			// setting request info
			String getUrl = url;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			//InputStream  getMethod =setGetMethod(getUrl).getResponseBodyAsStream();
			
			//System.out.println(getMethod.toString());
			tools.verifyResponse(setGetMethod(getUrl),
					expectGetMessage);
			

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
		}
	}
	
	/*
	 * 
	 *  
	 *  根据appkey获得appid 
	 *  存在的appkey 
	 *  返回appid=2
	 *  
	 */
	@Test
	public void test_TFS_RestfulUserDefinedGetAppidByExistAppkey2Test() {

		VerifyTool tools = new VerifyTool();
		/* init tair */
		//Tair tairServer = tair_01;
		/* set base url */
		TFS tfsServer = tfs_NginxB01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v2/" + tfsServer.getTfs_app_key()+"/appid";
		System.out.println(url);


		/* set expect response message */
		
		Map<String, String> expectGetMessage = new HashMap<String, String>();
		expectGetMessage.put("Content-Type", "application/json");
		expectGetMessage.put("body", "\"APP_ID\""+":"+" \"2\"");
		/* ---------------------------------------------- */
		// expectGetMessage.put("Content-Type", "image/x-ms-bmp");
		/* ---------------------------------------------- */

		try {

			/* set get method request */
			// setting request info
			String getUrl = url;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			//InputStream  getMethod =setGetMethod(getUrl).getResponseBodyAsStream();
			
			//System.out.println(getMethod.toString());
			tools.verifyResponse(setGetMethod(getUrl),
					expectGetMessage);
			

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
		}
	}
	
	
	
	
}
