package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_RcServer_Test_Distance_Test extends BaseCase {

	/* 测试距离计算
	 * app_key: tfsNginxIPMap01, app_id: 7
	 * app_id:7 source_ip:10.*.*.*,  turn_ip:10.232.36.203
	 * 构造一个查询请求，Nginx将前往T1M（ns:10.232.36.202）上查询
	 * 检查nginx log日志判断nginx是否到T1M上获取数据
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Distance_Test_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxIPMap01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int expectStatu = 1;
		
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		
		/* nginx log direct */
		String errorLog = "/" + NGINX.getServer_file_directory() + "/logs/error.log";
		String accessLog = "/" + NGINX.getServer_file_directory() + "/logs/access.log";
		String pid = "/" + NGINX.getServer_file_directory() + "/logs/nginx.pid";
		String readRetry202 = "read/stat retry: [0-9], select nameserver: 10.232.36.202:5202";
		String writeRetry209 = "write retry: [0-9], select nameserver: 10.232.36.209:6209";
		String writeRetry202 = "write retry: [0-9], select nameserver: 10.232.36.202:5202";
		String Retry202_1 = "select nameserver: 10.232.36.202:5202";
		String readRetry209 = "read/stat retry: [0-9], select nameserver: 10.232.36.209:6209";
		String Retry209_1 = "select nameserver: 10.232.36.209:6209";
		String readRetry = "read/stat retry:";
		String writeRetry = "write retry:";

		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteRetry = "unlink, select";

		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "0", "");

			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "0", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry209, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithOutSuffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/* 
	 * 测试距离计算
	 * app_key: tfsNginxIPMap02, app_id: 8
	 * app_id:8 source_ip:10.*.*.*,  turn_ip:10.232.36.210
	 * 构造一个查询请求，Nginx将前往T1M（ns:10.232.36.209）上查询
	 * 检查nginx log日志判断nginx是否到T1B上获取数据
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Distance_Test_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxIPMap02;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		int expectStatu = 1;
		
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		

		/* nginx log direct */
		String errorLog = "/" + NGINX.getServer_file_directory() + "/logs/error.log";
		String accessLog = "/" + NGINX.getServer_file_directory() + "/logs/access.log";
		String pid = "/" + NGINX.getServer_file_directory() + "/logs/nginx.pid";
		String readRetry202 = "read/stat retry: [0-9], select nameserver: 10.232.36.202:5202";
		String writeRetry209 = "write retry: [0-9], select nameserver: 10.232.36.209:6209";
		String writeRetry202 = "write retry: [0-9], select nameserver: 10.232.36.202:5202";
		String Retry202_1 = "select nameserver: 10.232.36.202:5202";
		String readRetry209 = "read/stat retry: [0-9], select nameserver: 10.232.36.209:6209";
		String Retry209_1 = "select nameserver: 10.232.36.209:6209";
		String readRetry = "read/stat retry:";
		String writeRetry = "write retry:";

		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteRetry = "unlink, select";
		
		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat /" + NGINX.getServer_file_directory() + "/logs/nginx.pid`", "", "");

			
			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
	//		tools.showResponse(setPostMethod(postUrl, localFile));
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */
			// setting request info
			String getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			TimeUnit.SECONDS.sleep(5);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
		
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			// 验证删除及删除顺序 ，从T1M删除，再同步到T1B删除
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithSuffix, suffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			

			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */
			// setting request info
			postUrl = url ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			

			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */
			// setting request info
			deleteUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			// use java client verify the status info 
			tools.verifyTFSFileStatu(tfsServer, tfsFileNameWithOutSuffix, expectStatu);
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			

			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
