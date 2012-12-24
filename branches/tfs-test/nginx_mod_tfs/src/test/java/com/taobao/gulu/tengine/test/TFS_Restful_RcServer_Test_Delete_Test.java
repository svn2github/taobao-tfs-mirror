package com.taobao.gulu.tengine.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

public class TFS_Restful_RcServer_Test_Delete_Test extends BaseCase {

	/* 
	 * 特殊数据删除
	 * 在T2M[T2M（读）（独立的物理集群，存放以前的老数据）]中存放一些T2数据
	 * 通过访问逻辑集群的Appkey去删除T2数据，验证是否能够正常的删除
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_01() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxC01;
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		

		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/access.log", "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat /" + NGINX.getServer_file_directory() + "/logs/nginx.pid`", "", "");
			
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
			String deleteUrl = url + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
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
			deleteUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
		
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/* 
	 * 特殊数据删除
	 * 在T2M[T2M（读）（独立的物理集群，存放以前的老数据）]中存放一些T2数据
	 * 反删除;通过访问逻辑集群的Appkey去删除T2数据，然后再将这个数据反删除，
	 * 看是否能够恢复该数据
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_02() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxC01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		/* put file into tfs use nginx client */
		String suffix = ".2k";
		String localFile = _2kFile;
		String type_undelete = "2";
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
			String deleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
			
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* set undelete method request */
			// setting request info
			String undeleteUrl = url + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix + "&type=" + type_undelete;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do undelete file aciton */
			tools.verifyResponse(setDeleteMethod(undeleteUrl), expectDeleteMessage);
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithSuffix ;
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
			deleteUrl = url + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectDeleteMessage);
		
			
			/* set get method request */
			// setting request info
			getUrl = url + "/" + tfsFileNameWithOutSuffix ;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* set undelete method request */
			// setting request info
			undeleteUrl = url + "/" + tfsFileNameWithOutSuffix + "?type=" + type_undelete;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do undelete file aciton */
			tools.verifyResponse(setDeleteMethod(undeleteUrl), expectDeleteMessage);
			
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

	/* 
	 * 特殊数据删除
	 * 在T2M[T2M（读）（独立的物理集群，存放以前的老数据）]中存放一些T2数据
	 * 读取：通过访问逻辑集群的Appkey去读取T2数据，验证是否能够正常的读取
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_03() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxC01;
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
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");
		

		try {
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/access.log", "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat /" + NGINX.getServer_file_directory() + "/logs/nginx.pid`", "", "");
			
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
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* 
	 * 特殊数据删除
	 * 单向写
	 * 物理集群中存在T1M写和T1B读，用普通appkey向逻辑集群A存数据
	 * 数据会先存到T1M，再同步到T1B
	 * 用appkey:tfs_NginxIPMap02删除这个数据，按就近原则会从T1B删，
	 * 但实际会从T1M删，再同步删除T1B
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_04() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		TFS tfsServer_IP = tfs_NginxIPMap02;
		String url_IP = NGINX.getRoot_url_adress();
		url_IP = url_IP + "v1/" + tfsServer_IP.getTfs_app_key();
		
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
		
		Map<String, String> expectSuccessMessage = new HashMap<String, String>();
		expectSuccessMessage.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "404");

		/* nginx log direct */
		String errorLog = "/" + NGINX.getServer_file_directory() + "/logs/error.log";
		String accessLog = "/" + NGINX.getServer_file_directory() + "/logs/access.log";
		String pid = "/" + NGINX.getServer_file_directory() + "/logs/nginx.pid";
		String readRetry202 = "read/stat retry: [0-9], select nameserver: 10.232.36.202:5202";
		String writeRetry202 = "write retry: [0-9], select nameserver: 10.232.36.202:5202";
		String writeRetry209 = "write retry: [0-9], select nameserver: 10.232.36.202:6209";
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
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 验证数据同步及就近读取T1B内的数据是否成功
			// setting request info
			String getUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "0", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			String deleteUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
			
		
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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

			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 验证数据同步及就近读取T1B内的数据是否成功
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "0", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			deleteUrl = url_IP + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "0", "");
		
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponse(setGetMethod(getUrl), expectGetMessageAfterDelete);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry209_1, "");
			/* clean the nginx log */
//			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
//			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
//			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0429, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, INITMYSQLTABLES, "", "");
				NGINX.restart();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	
	/* 
	 * 特殊数据删除
	 * 单向写,T1B 向 T1M备份， T1M不向T1B备份
	 * 物理集群中存在T1M读和T1B写，用普通appkey向逻辑集群A存数据
	 * 数据会先存到T1B，再同步到T1M
	 * 用appkey:tfs_NginxIPMap01删除这个数据，会从T1M删，不会删除T1B的数据
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_05() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		url = url + "v1/" + tfsServer.getTfs_app_key();
		
		TFS tfsServer_IP = tfs_NginxIPMap01;
		String url_IP = NGINX.getRoot_url_adress();
		url_IP = url_IP + "v1/" + tfsServer_IP.getTfs_app_key();
		
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
		
		Map<String, String> expectSuccessMessage = new HashMap<String, String>();
		expectSuccessMessage.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
		Map<String, String> expectGetMessageAfterDelete = new HashMap<String, String>();
		expectGetMessageAfterDelete.put("status", "200");

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
			recoverTFStoT1BBackupToT1M();
			/* modify mysql */
			tools.verifyCMD(SERVER0429, UPDATE_PHYSICS_T1B_WRITE_T1M_READ, "", "");
			

			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			NGINX.restart();
			System.out.println("Sleep 10s");
			TimeUnit.SECONDS.sleep(10);
			
			/* set post method request */
			// setting request info
			String postUrl = url + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 验证数据同步到T1M及就近读取T1M内的数据是否成功
			// setting request info
			String getUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
		//	tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");			
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			String deleteUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			

			/* set get method request */ // 验证重试 及重试顺序  T1M中已经删除，T1B中还存在
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 验证数据同步及就近读取T1M内的数据是否成功
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
	//		tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			deleteUrl = url_IP + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);

			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP + "/" + tfsFileNameWithOutSuffix ;
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
		}finally{
			/* recover mysql */
			try {
				tools.verifyCMD(SERVER0429, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, INITMYSQLTABLES, "", "");
				recoverTFS();
				NGINX.restart();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/* 
	 * 特殊数据删除
	 * 双向写
	 * 物理集群中存在T1M写和T1B写，用appkey：tfs_NginxIPMap01向逻辑集群A存数据
	 * 通过就近访问T1M集群的appkey:tfs_NginxIPMap01去存储该数据,数据会先存到T1M，再同步到T1B
	 * 用appkey:tfs_NginxIPMap02删除这个数据，会从T1M删，再同步删除T1B
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_06() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		
		TFS tfsServer_IP01 = tfs_NginxIPMap01;
		String url_IP01 = NGINX.getRoot_url_adress();
		url_IP01 = url_IP01 + "v1/" + tfsServer_IP01.getTfs_app_key();
		
		TFS tfsServer_IP02 = tfs_NginxIPMap02;
		String url_IP02 = NGINX.getRoot_url_adress();
		url_IP02 = url_IP02 + "v1/" + tfsServer_IP02.getTfs_app_key();
		
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
		
		Map<String, String> expectSuccessMessage = new HashMap<String, String>();
		expectSuccessMessage.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
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
			
			/* modify mysql */
			tools.verifyCMD(SERVER0429, UPDATE_PHYSICS_T1B_WRITE_T1M_WRITE, "", "");
			recoverTFStoDoubleWrite();
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/access.log", "", "");
			NGINX.restart();
			
			/* set post method request */
			// setting request info
			String postUrl = url_IP01 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			

			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry202_1, "");
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */ // 验证数据同步及就近读取T1B内的数据是否成功
			// setting request info
			String getUrl = url_IP02 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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
			
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			String deleteUrl = url_IP02 + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP02 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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
			
			/* set post method request */
			// setting request info
			postUrl = url_IP01 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */ // 验证数据同步及就近读取T1B内的数据是否成功
			// setting request info
			getUrl = url_IP02 + "/" + tfsFileNameWithOutSuffix;
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

			
			
			/* set delete method request */ // 验证从T1M上删除数据，且删除成功
			// setting request info
			deleteUrl = url_IP02 + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete202 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP02 + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
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
		}finally{
			/* recover mysql */
			try {
				recoverTFS();
				tools.verifyCMD(SERVER0429, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, INITMYSQLTABLES, "", "");
				NGINX.restart();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/* 
	 * 特殊数据删除
	 * 双向写
	 * 物理集群中存在T1M写和T1B写，用appkey：tfs_NginxIPMap02向逻辑集群A存数据
	 * 通过就近访问T1B集群的appkey:tfs_NginxIPMap02去存储该数据,数据会先存到T1B，再同步到T1M
	 * 用appkey:tfs_NginxIPMap01删除这个数据，会从T1B删，再同步删除T1M
	 * 
	 */
	@Test
	public void test_TFS_Restful_RcServer_Test_Delete_Test_07() {

		VerifyTool tools = new VerifyTool();
		String tfsFileNameWithSuffix = null;
		String tfsFileNameWithOutSuffix = null;

		/* set base url */
		
		TFS tfsServer_IP01 = tfs_NginxIPMap01;
		String url_IP01 = NGINX.getRoot_url_adress();
		url_IP01 = url_IP01 + "v1/" + tfsServer_IP01.getTfs_app_key();
		
		TFS tfsServer_IP02 = tfs_NginxIPMap02;
		String url_IP02 = NGINX.getRoot_url_adress();
		url_IP02 = url_IP02 + "v1/" + tfsServer_IP02.getTfs_app_key();
		
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
		
		Map<String, String> expectSuccessMessage = new HashMap<String, String>();
		expectSuccessMessage.put("status", "200");
		
		Map<String, String> expectErrorMessage = new HashMap<String, String>();
		expectErrorMessage.put("status", "500");
		
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
		String delete209 = "unlink, select nameserver: 10.232.36.209:6209";
		String deleteRetry = "unlink, select";
		

		try {
			
			/* modify mysql */
			tools.verifyCMD(SERVER0429, UPDATE_PHYSICS_T1B_WRITE_T1M_WRITE, "", "");
			recoverTFStoDoubleWrite();
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/error.log", "", "");
			tools.verifyCMD(SERVER0432, "rm -fr /" + NGINX.getServer_file_directory() + "/logs/access.log", "", "");
			NGINX.restart();
			
			/* set post method request */
			// setting request info
			String postUrl = url_IP02 + "?suffix=" + suffix;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name with suffix is  : " + tfsFileNameWithSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */ // 验证数据同步及就近读取T1M内的数据是否成功
			// setting request info
			String getUrl = url_IP01 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
			System.out.println("the getUrl : " + getUrl);

			/* do get file action */
			tools.verifyResponseBodyWithLocalFile(setGetMethod(getUrl), expectGetMessage);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + readRetry202 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + readRetry + "'", Retry202_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set delete method request */ // 验证从T1B上删除数据，且删除成功
			// setting request info
			String deleteUrl = url_IP01 + "/" + tfsFileNameWithSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			
			/* verify log info */
//			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry202 + "' -c", "1", "");
//			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry202_1, "");
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "1", "");
			
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithSuffix + "?suffix=" + suffix;
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
			postUrl = url_IP02 ;
			System.out.println("the postUrl : " + postUrl);
			// set the post method
			/* do post file action */
			tfsFileNameWithOutSuffix = tools.verifyResponseAndGetTFSFileName(setPostMethod(postUrl, localFile), expectPostMessage);
			System.out.println("the tfs file name without suffix is  : " + tfsFileNameWithOutSuffix);
			
			/* verify log info */
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + writeRetry209 + "' -c", "1", "");
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep -m 1 '" + writeRetry + "'", Retry209_1, "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	

			
			/* set get method request */ // 验证数据同步及就近读取T1M内的数据是否成功
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithOutSuffix;
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
			
			/* set delete method request */ // 验证从T1B上删除数据，且删除成功
			// setting request info
			deleteUrl = url_IP01 + "/" + tfsFileNameWithOutSuffix;
			System.out.println("the deleteUrl : " + deleteUrl);
			
			/* do delete file aciton */
			tools.verifyResponse(setDeleteMethod(deleteUrl), expectSuccessMessage);
			// 验证删除及删除顺序
			tools.verifyCMD(SERVER0432, "cat " + errorLog + "| grep '" + delete209 + "' -c", "1", "");
			/* clean the nginx log */
			tools.verifyCMD(SERVER0432, "rm -fr " + errorLog, "", "");
			tools.verifyCMD(SERVER0432, "rm -fr " + accessLog, "", "");
			tools.verifyCMD(SERVER0432, "kill -USR1 `cat " + pid + "`", "", "");	
			
			/* set get method request */ // 验证重试 及重试顺序
			// setting request info
			getUrl = url_IP01 + "/" + tfsFileNameWithOutSuffix + "?suffix=" + suffix;
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
		}finally{
			/* recover mysql */
			try {
				recoverTFS();
				tools.verifyCMD(SERVER0429, DROPMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, CREATEMYSQLTABLES, "", "");
				tools.verifyCMD(SERVER0429, INITMYSQLTABLES, "", "");
				NGINX.restart();
				TimeUnit.SECONDS.sleep(60);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
//	@Test
//	public void test_11() throws Exception {
//		recoverTFS();
////		startTFS();
//
//		recoverTFStoDoubleWrite();
//		recoverTFStoDoubleBackup();
//	}
	
}
