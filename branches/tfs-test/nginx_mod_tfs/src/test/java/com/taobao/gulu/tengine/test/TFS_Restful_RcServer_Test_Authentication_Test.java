package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_RcServer_Test_Authentication_Test extends BaseCase {

	/* 逻辑集群读/写/不可访问权限
	 * 逻辑集群A:读；物理集群A:T1M写，T1B读，T2M读；
	 * 访问集群A可读，不可写，不可删除
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Authentication_Test_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

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

		/* nginx log direct */
		String errorLog = NGINX.getServer_file_directory() + "logs/error.log";
		String accessLog = NGINX.getServer_file_directory() + "logs/access.log";
		String pid = NGINX.getServer_file_directory() + "logs/nginx.pid";
		
		
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteRetry = "unlink, select";
		
		String read = "read/stat";
		String write = "write";
		
		String Retry202_1 = "nameserver: 10.232.36.202:5202";
		String Retry209_1 = "nameserver: 10.232.36.209:6209";
		
		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			String postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	

			/* set post method request */
			// setting request info
			postUrl = url_A ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			
			/* modify mysql */
			tools.verifyCMD(SERVER0435, UPDATE_LOGIC_A_READ, "", "");
			
			
			NGINX.restart();
			TimeUnit.SECONDS.sleep(15);
			/* set post method request */
			// setting request info
			postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */
			// setting request info
			String getUrl = url_A + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + read + "'", Retry202_1, "");
			
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url_A + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectErrorMessage);
			
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + read + "'", Retry202_1, "");
			
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set post method request */
			// setting request info
			postUrl = url_A ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
//			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + read + "'", Retry202_1, "");
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			deleteUrl = url_A + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectErrorMessage);
		
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
//			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + read + "'", Retry202_1, "");
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				NGINX.restart();
				TimeUnit.SECONDS.sleep(15);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/* 逻辑集群读/写/不可访问权限
	 * 逻辑集群A:不可访问；物理集群A:T1M写，T1B读，T2M读；
	 * 访问集群A不可访问，不可删除
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Authentication_Test_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */

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
		
		/* nginx log direct */
		String errorLog = NGINX.getServer_file_directory() + "logs/error.log";
		String accessLog = NGINX.getServer_file_directory() + "logs/access.log";
		String pid = NGINX.getServer_file_directory() + "logs/nginx.pid";
		
		
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteRetry = "unlink, select";
		
		String read = "read/stat";
		String write = "write";
		
		String Retry202_1 = "nameserver: 10.232.36.202:5202";
		String Retry209_1 = "nameserver: 10.232.36.209:6209";
		
		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			String postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set post method request */
			// setting request info
			postUrl = url_A ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* modify mysql */
			tools.verifyCMD(SERVER0435, UPDATE_LOGIC_A_UNUSE, "", "");
			
			NGINX.restart();
			TimeUnit.SECONDS.sleep(15);
			/* set post method request */
			// setting request info
			postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* verify log info */  // 没有相关log记录
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");
		
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */
			// setting request info
			String getUrl = url_A + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url_A + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectErrorMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
	//		tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			postUrl = url_A ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");
	//		tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
	//		tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */
			// setting request info
			deleteUrl = url_A + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectErrorMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
		
			
			/* set get method request */
			// setting request info
			getUrl = url_A + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
	//		tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				NGINX.restart();
				TimeUnit.SECONDS.sleep(15);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	/* 物理集群读/写/不可访问权限
	 * 物理集群A都只读；逻辑集群A为可写
	 * 都只可读，可删除，不可写
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Authentication_Test_03() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffixC01 = null;
		String tfsFileNameWithOutSuffixC01  = null;
		String tfsFileNameWithSuffixD01 = null;
		String tfsFileNameWithOutSuffixD01 = null;
		String tfsFileNameWithSuffixT1M = null;
		String tfsFileNameWithOutSuffixT1M = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		TFS tfsServerC01 = tfs_NginxC01;
		TFS tfsServerD01 = tfs_NginxD01;
		TFS tfsServerT1M = tfs_NginxIPMap01;
		String url = NGINX.getRoot_url_adress();
		String urlC01 = url + "v1/" + tfsServerC01.getTfs_app_key();
		String urlD01 = url + "v1/" + tfsServerD01.getTfs_app_key();
		String urlT1M = url + "v1/" + tfsServerT1M.getTfs_app_key();
		String URL = url + "v1/" + tfsServer.getTfs_app_key();
		
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

		/* nginx log direct */
		String errorLog = NGINX.getServer_file_directory() + "logs/error.log";
		String accessLog = NGINX.getServer_file_directory() + "logs/access.log";
		String pid = NGINX.getServer_file_directory() + "logs/nginx.pid";
		
		
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteT2M = "unlink, select nameserver: 10.232.36.202:7202";
		String deleteRetry = "unlink, select";
		
		String read = "read/stat";
		String write = "write";
		String unlink = "unlink";
		
		String Retry202_1 = "nameserver: 10.232.36.202:5202";
		String Retry209_1 = "nameserver: 10.232.36.209:6209";
		String RetryT2M = "nameserver: 10.232.36.202:7202";
		
		try {
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			String postUrl = urlC01 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixC01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixC01);
			
			postUrl = urlC01 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixC01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixC01);
			
			postUrl = urlD01 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixD01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixD01);
			
			postUrl = urlD01 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixD01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixD01);
			
			postUrl = urlT1M + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixT1M = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixT1M);
			
			postUrl = urlT1M ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixT1M = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixT1M);
			
			/* verify log info */
			
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "2", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "2", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+RetryT2M+"' -c", "2", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* modify mysql */
			tools.verifyCMD(SERVER0435, UPDATE_PHYSICS_T1B_READ_T1M_READ, "", "");
			
			NGINX.restart();
			TimeUnit.SECONDS.sleep(15);
			
			/* set get method request */
			// setting request info
			String getUrl = URL + "/" + tfsFileNameWithSuffixC01 + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+RetryT2M+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			getUrl = URL + "/" + tfsFileNameWithSuffixD01 + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+RetryT2M+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			getUrl = URL + "/" + tfsFileNameWithSuffixT1M + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+RetryT2M+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "1", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			//删除都好像有些问题
			String deleteUrl = URL + "/" + tfsFileNameWithSuffixC01;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + deleteT2M + "' -c", "1", "");
//			/* clean the nginx log */
//			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
//			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
//			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
//			
			deleteUrl = urlD01 + "/" + tfsFileNameWithSuffixD01;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + unlink + "'|grep '"+Retry209_1+"' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			deleteUrl = URL + "/" + tfsFileNameWithSuffixT1M;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set post method request */ //with suffix
			
			
			postUrl = URL + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
		
			
			postUrl = URL ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* verify log info */  // 没有相关log记录
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+RetryT2M+"' -c", "0", "");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				NGINX.restart();
				TimeUnit.SECONDS.sleep(15);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/* 物理集群读/写/不可访问权限
	 * 物理集群A不可用；逻辑集群A为可写
	 * 不可读，不可删除，不可写
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Authentication_Test_04() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffixC01 = null;
		String tfsFileNameWithOutSuffixC01  = null;
		String tfsFileNameWithSuffixD01 = null;
		String tfsFileNameWithOutSuffixD01 = null;
		String tfsFileNameWithSuffixT1M = null;
		String tfsFileNameWithOutSuffixT1M = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		TFS tfsServerC01 = tfs_NginxC01;
		TFS tfsServerD01 = tfs_NginxD01;
		TFS tfsServerT1M = tfs_NginxIPMap01;
		String url = NGINX.getRoot_url_adress();
		String urlC01 = url + "v1/" + tfsServerC01.getTfs_app_key();
		String urlD01 = url + "v1/" + tfsServerD01.getTfs_app_key();
		String urlT1M = url + "v1/" + tfsServerT1M.getTfs_app_key();
		String URL = url + "v1/" + tfsServer.getTfs_app_key();
		
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
		expectDeleteMessage.put("status", "404");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "404");
	

		/* nginx log direct */
		String errorLog = NGINX.getServer_file_directory() + "logs/error.log";
		String accessLog = NGINX.getServer_file_directory() + "logs/access.log";
		String pid = NGINX.getServer_file_directory() + "logs/nginx.pid";
		
		
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteT2M = "unlink, select nameserver: 10.232.36.202:7202";
		String deleteRetry = "unlink, select";
		
		String read = "read/stat";
		String write = "write";
		
		String Retry202_1 = "nameserver: 10.232.36.202:5202";
		String Retry209_1 = "nameserver: 10.232.36.209:6209";
		String RetryT2M = "nameserver: 10.232.36.202:7202";

		try {

			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			/* set post method request */
			// setting request info
			String postUrl = urlC01 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixC01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixC01);
			
			postUrl = urlC01 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixC01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixC01);
			
			postUrl = urlD01 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixD01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixD01);
			
			postUrl = urlD01 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixD01 = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixD01);
			
			postUrl = urlT1M + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffixT1M = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffixT1M);
			
			postUrl = urlT1M ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffixT1M = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffixT1M);
			
			/* modify mysql */
		//	tools.verifyCMD(SERVER0435, UPDATE_LOGIC_A_READ, "", "");
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "2", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "2", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+RetryT2M+"' -c", "2", "");

			
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* modify mysql */
			tools.verifyCMD(SERVER0435, UPDATE_PHYSICS_UNUSE, "", "");
			TimeUnit.SECONDS.sleep(15);
			
			NGINX.restart();
			
			
			
			
			String getUrl = URL + "/" + tfsFileNameWithSuffixT1M + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectErrorMessage);
			
			
			
			/* set delete method request */
			// setting request info
			String deleteUrl = URL + "/" + tfsFileNameWithSuffixC01;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
		
			
			deleteUrl = URL + "/" + tfsFileNameWithSuffixD01;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			deleteUrl = URL + "/" + tfsFileNameWithSuffixT1M;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			
			/* set post method request */ //with suffix
			// setting request info
			postUrl = URL + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			
			postUrl = urlT1M + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			//without suffix
			postUrl = URL ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
	
			
			postUrl = urlT1M ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tools.verifyResponse(setPostMethod(postUrl, localFile), expectErrorMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+Retry209_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + write + "'|grep '"+RetryT2M+"' -c", "0", "");
			/* verify log info */  // 没有相关log记录
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+RetryT2M+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry202_1+"' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + read + "'|grep '"+Retry209_1+"' -c", "0", "");
			// 验证删除及删除顺序, 没有相关log记录
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			tools.verifyCMD(SERVER0435, "cat " + errorLog + "| grep '" + deleteT2M + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0435, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0435, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0435, "kill -USR1 `cat " + pid + "`", "", "");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0435, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0435, INITMYSQLTABLES_META_ROOT_INFO, "", "");
				NGINX.restart();
				TimeUnit.SECONDS.sleep(15);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
