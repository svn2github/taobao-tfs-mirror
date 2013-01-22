package com.taobao.gulu.tengine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Assert;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tools.VerifyTool;


	public	class Delete extends BaseCase implements Callable<Integer> 
	{    
	 
	    @Override  
	    public Integer call() throws Exception 
	    { 
	    	VerifyTool tools = new VerifyTool();
			String tfsFileNameWithSuffix = null;
			String tfsFileNameWithOutSuffix = null;

			/* set base url */
			TFS tfsServer = tfs_NginxA03;
			String url = NGINX.getRoot_url_adress();
			url = url + "v1/" + tfsServer.getTfs_app_key();
			
			/* put file into tfs use nginx client */
			String suffix = ".bmp";
			String localFile = bmpFile;
			int expectStatu = 0;
//			int expectStatu_delete = 1;
			int expectStatu_hide = 4;
			String type_hide = "4";
			String unhide = "0";
//			String type_meta = "1";
			
			/* set expect response message */
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("Content-Type", "application/json");
			
			Map<String, String> expectGetMessage = new HashMap<String, String>();
			expectGetMessage.put("body", localFile);
			/* ---------------------------------------------- */
			//expectGetMessage.put("Content-Type", "image/x-ms-bmp");
			/* ---------------------------------------------- */
			
			Map<String, String> expectDeleteErrorMessage = new HashMap<String, String>();
			expectDeleteErrorMessage.put("status", "404");
			
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");
			
			Map<String, String> expectGetErrorMessage = new HashMap<String, String>();
			expectGetErrorMessage.put("status", "404");
			
			try
			{
				for(int i=0;i<500;i++)
				{
					String postUrl = url + "?suffix=" + suffix;
					System.out.println("the postUrl : " + postUrl);
					// set the post method
					/* do post file action */
					tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
					System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
					put2TfsKeys.add(tfsFileNameWithSuffix);
					
					/* set get method request */
					// setting request info
					String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
					System.out.println("the getUrl : " + getUrl);

					/* do get file action */
					tools.verifyResponse(setGetMethod(getUrl));
					
					/* set delete method request */
					// setting request info undelete the tfs file
			//		String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_hide;
			//		System.out.println("the deleteUrl : " + deleteUrl);
					
					/* do delete file aciton */
//					tools.showResponse(setDeleteMethod(deleteUrl));
			//		tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); //TimeUnit.SECONDS.sleep(20);
					
					// use java client verify the status info 
			//		tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu_hide);
					//TimeUnit.SECONDS.sleep(20);
					
					/* do get file action */
					System.out.println("the getUrl : " + getUrl);
					tools.verifyResponse(setGetMethod(getUrl));
					
					/* set delete method request */
					// setting request info undelete the tfs file
			//		deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&hide=" + unhide;
			//		System.out.println("the deleteUrl : " + deleteUrl);
					
					/* do delete file aciton */
//					tools.showResponse(setDeleteMethod(deleteUrl));
			//		tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage); //TimeUnit.SECONDS.sleep(20);
					
					// use java client verify the status info 
			//		tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
					
					/* do get file action */
					System.out.println("the getUrl : " + getUrl);
					tools.verifyResponse(setGetMethod(getUrl));
				}
			}
			
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace(); 
				Assert.assertTrue(false);
			}
	    	return 1;
	    }   
	}   

