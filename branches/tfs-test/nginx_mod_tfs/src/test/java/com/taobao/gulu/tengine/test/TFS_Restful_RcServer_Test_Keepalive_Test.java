package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_RcServer_Test_Keepalive_Test extends BaseCase
{
	@Test
	public void test_TFS_Restful_RcServer_Test_Authentication_Test_01() 
	{
		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;

		/* set base url */
//		TFS tfsServer = tfs_Nginx01;
//		String url = NGINX.getRoot_url_adress();
//		url = url + "v1/" + tfsServer.getTfs_app_key();
//		
		TFS tfsServer_A = tfs_NginxA01;
		String url_A = NGINX.getRoot_url_adress();
		url_A = url_A + "v1/" + tfsServer_A.getTfs_app_key();
		
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
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		try 
		{
			String postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			tools.verifyCMD(SERVER0435, UPDATE_DUPLICATE_SERVER_UNUSE, "", "");
			
			TimeUnit.SECONDS.sleep(15);
			
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);		
		}
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
		
		finally
		{
			/* recover mysql */
			try 
			{
				tools.verifyCMD(SERVER0435, UPDATE_DUPLICATE_SERVER, "", "");
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace(); Assert.assertTrue(false);
			}
		}
		
	}

}
