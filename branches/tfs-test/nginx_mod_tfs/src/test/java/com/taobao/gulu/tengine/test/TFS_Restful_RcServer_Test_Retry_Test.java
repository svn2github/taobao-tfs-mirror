package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_RcServer_Test_Retry_Test extends BaseCase {

	/* 重试
	 * 配置T1M集群与T1B集群之间不互为主备（不同步数据）
	 * 在T1M中不存，T1B中写入数据
	 * 通过就近访问T1B集群的Appkey去获取该数据（读/stat），数据获取失败，重试T1M集群
	 * 检查Nginx log日志，验证是否有重试操作
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Retry_Test_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;
		
		/* nginx log direct */
		String errorLog = "/" + NGINX.getServer_file_directory() + "/logs/error.log";
		String accessLog = "/" + NGINX.getServer_file_directory() + "/logs/access.log";
		String pid = "/" + NGINX.getServer_file_directory() + "/logs/nginx.pid";
		String readRetry202 = "read/stat retry: [0-9], select nameserver: 10.232.36.202:5202";
		String readRetry202_1 = "select nameserver: 10.232.36.202:5202";
		String readRetry209 = "read/stat retry: [0-9], select nameserver: 10.232.36.209:6209";
		String readRetry209_1 = "select nameserver: 10.232.36.209:6209";
		String readRetry = "read/stat retry:";
		String writeRetry209 = "write retry: [0-9], select nameserver: 10.232.36.209:6209";
		String writeRetry202 = "write retry: [0-9], select nameserver: 10.232.36.202:5202";
		
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209_D = "unlink, no master found, select nameserver: 10.232.36.209:6209";
		String deleteRetry = "unlink, select";

		/* set base url */
		TFS tfsServer_D = tfs_NginxD01;
		String url_D = NGINX.getRoot_url_adress();
		url_D = url_D + "v1/" + tfsServer_D.getTfs_app_key();
		
		TFS tfsServer_IP202 = tfs_NginxIPMap01;
		String url_IP202 = NGINX.getRoot_url_adress();
		url_IP202 = url_IP202 + "v1/" + tfsServer_IP202.getTfs_app_key();
		
		TFS tfsServer_IP209 = tfs_NginxIPMap02;
		String url_IP209 = NGINX.getRoot_url_adress();
		url_IP209 = url_IP209 + "v1/" + tfsServer_IP209.getTfs_app_key();
		
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
		
		Map<String, String> expectGetMessage404 = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		

		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");

			/* set post method request */ // 单独写入T1B
			// setting request info
			String postUrl = url_D + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");

			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1M, 查看重试日志
			// setting request info
			String getUrl = url_IP202 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1B, 查看重试日志
			// setting request info
			getUrl = url_IP209 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */ // 删除数据
			// setting request info
			String deleteUrl = url_D + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209_D + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			
			/* set get method request */ // 就近访问T1M, 查看重试日志
			// setting request info
			getUrl = url_IP202 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1B, 查看重试日志
			// setting request info
			getUrl = url_IP209 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set post method request */ // 单独写T1B
			// setting request info
			postUrl = url_D ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1M 查看重试日志
			// setting request info
			getUrl = url_IP202 + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1B, 查看重试日志
			// setting request info
			getUrl = url_IP209 + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */ // 删除数据
			// setting request info
			deleteUrl = url_D + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209_D + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1M  查看重试日志
			// setting request info
			getUrl = url_IP202 + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 就近访问T1B  查看重试日志
			// setting request info
			getUrl = url_IP209 + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	


	/* 重试
	 * block存在，数据不存在，重试
	 * 写入数据，返回文件名
	 * 删除数据
	 * 用两个距离appkey去读该被删除的文件，验证重试及重试顺序
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Retry_Test_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* nginx log direct */
		String errorLog = "/" + NGINX.getServer_file_directory() + "/logs/error.log";
		String accessLog = "/" + NGINX.getServer_file_directory() + "/logs/access.log";
		String pid = "/" + NGINX.getServer_file_directory() + "/logs/nginx.pid";
		String readRetry202 = "read/stat retry: [0-9], select nameserver: 10.232.36.202:5202";
		String readRetry202_1 = "select nameserver: 10.232.36.202:5202";
		String readRetry209 = "read/stat retry: [0-9], select nameserver: 10.232.36.209:6209";
		String readRetry209_1 = "select nameserver: 10.232.36.209:6209";
		String readRetry = "read/stat retry:";
		String delete202 = "unlink, select nameserver: 10.232.36.202:5202";
		String delete209 = "unlink, select nameserver: 10.232.36.202:6209";
		String deleteRetry = "unlink, select";
		
		/* set base url */
		TFS tfsServer_A = tfs_NginxA01;
		String url_A = NGINX.getRoot_url_adress();
		url_A = url_A + "v1/" + tfsServer_A.getTfs_app_key();
		
		TFS tfsServer_IP01 = tfs_NginxIPMap01;
		String url_IP = NGINX.getRoot_url_adress();
		String url_IP01 = url_IP + "v1/" + tfsServer_IP01.getTfs_app_key();
		
		TFS tfsServer_IP02 = tfs_NginxIPMap02;
		String url_IP02 = url_IP + "v1/" + tfsServer_IP02.getTfs_app_key();
		
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
		

		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/access.log", "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat /" + NGINX.getServer_file_directory() + "/logs/nginx.pid`", "", "");

			/* set post method request */ // 单独写入T1M
			// setting request info
			String postUrl = url_A + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* set get method request */ // 访问, 查看重试日志
			// setting request info
			String getUrl = url_IP01 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			getUrl = url_IP02 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
	//		tools.showResponse(setGetMethod(getUrl));
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
		//	tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */ // 删除数据
			// setting request info
			String deleteUrl = url_A + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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
			tools.verifyTFSFileStatu(tfsServer_A, tfsFileNameWithSuffix, expectStatu);
			
			/* set get method request */ // 从逻辑集群A T1M访问删除数据,验证重试及重试顺序
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set get method request */ // 从逻辑集群A T1B访问删除数据,验证重试及重试顺序
			// setting request info
			getUrl = url_IP02 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			
			/* set post method request */ // 单独写入T1M
			// setting request info
			postUrl = url_A ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* set get method request */ // 访问, 查看重试日志
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			getUrl = url_IP02 + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */ // 删除数据
			// setting request info
			deleteUrl = url_A + "/" + tfsFileNameWithOutSuffix;
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
			tools.verifyTFSFileStatu(tfsServer_A, tfsFileNameWithOutSuffix, expectStatu);
			
			/* set get method request */ // 从逻辑集群A T1M访问删除数据,验证重试及重试顺序
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 从逻辑集群A T1B访问删除数据,验证重试及重试顺序
			// setting request info
			getUrl = url_IP02 + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);
			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", readRetry209_1, "");
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
