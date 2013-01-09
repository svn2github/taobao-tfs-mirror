package com.taobao.gulu.tengine.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONArray;
import org.json.*;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.namemeta.FileMetaInfo;
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
public class TFS_Restful_UserDefinedDir_Test extends BaseCase {
	/*
	 * 
	 * 
	 * 目录 的creat delete get ls mv等
	 */

	/*
	 * create dir 返回201
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateDirReturn201Test() 
	{

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_201create201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);

			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create dir 返回401 appid不是appkey对应的appid
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateDirReturn401Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/2" + "/1"
				+ "/dir" + "/dir_401";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "401");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create dir 返回400 appid不合法
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateDirReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/X" + "/1"
				+ "/dir" + "/dir_400";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "400");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create dir 返回404 父目录不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateDirReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_404/dir_404/";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "404");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create dir 返回403 超过最大子目录数、文件数、目录深度
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateDirReturn403Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url
				+ "v2/"
				+ tfsServer.getTfs_app_key()
				+ "/1"
				+ "/1"
				+ "/dir"
				+ "/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "403");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete dir 返回400 目录/文件名不合规范
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteDirReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/X" + "/1"
				+ "/dir" + "/dir_201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "400");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete dir 返回401 无权限（目前每个应用只对自己的appid下的空间拥有修改权限，包括创建/删除/移动目录/文件、写文件）
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteDirReturn401Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/2" + "/1"
				+ "/dir" + "/dir_201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "401");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete dir 返回403目录非空 目录中有目录
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteDirReturn403ExistDirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_403/dir_403";
		String urlMetaParent = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + "/dir_403";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "403");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			// create dir_403/dir_403
			tools.verifyResponse(setPostMethod(urlMetaParent),
					expectPostMessage);
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);
			tools.verifyResponse(setDeleteMethod(urlMetaParent),
					expectDeleteMessage);

			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlMetaParent),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete dir 返回403目录非空 目录中有文件
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteDirReturn403ExistFileTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/file/dir_403/localFile";
		String urlMetaParent = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir/dir_403";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "403");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			System.out.println("the postUrl   is : " + urlFile);
			/* do post file action */
			System.out.println("creat dir  begin");
			// create dir_403/dir_403
			tools.verifyResponse(setPostMethod(urlMetaParent),
					expectPostMessage);
			tools.verifyResponse(setPostMethod(urlFile + "?recursive=0"),
					expectPostMessage);
			// 删除 返回403 错误，有文件未删除
			tools.verifyResponse(setDeleteMethod(urlMetaParent),
					expectDeleteMessage);

			// 删除文件，后删除目录成功
			tools.verifyResponse(setDeleteMethod(urlFile),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlMetaParent),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete dir 返回404 目录或者父目录不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteDirReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_404/dir_404";
		String urlMetaParent = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + "/dir_404";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "404");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			String getMetaUrl = urlMeta;
			System.out.println("the getUrl   is : " + getMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);

			tools.verifyResponse(setDeleteMethod(urlMetaParent),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv dir 返回200 操作成功
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_srcmv200";
		String destDir = "/dir_destmv200";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir + "?recursive=0";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage404 = new HashMap<String, String>();
			expectDeleteMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectMvDirMessage200 = new HashMap<String, String>();
			expectMvDirMessage200.put("status", "200");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage200);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage404);
			tools.verifyResponse(setDeleteMethod(urlDestDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ****mv dir 返回200 操作成功 creat parent dir
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn200CreatParentDirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_srcmv2001";
		String destDir = "/dir_parent1/dir_dest/";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir + "?recursive=1";
		String urlDelDestDir= url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir;
		String urlDelDir_parent= url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir/dir_parent1";
		
		

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage404 = new HashMap<String, String>();
			expectDeleteMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectMvDirMessage200 = new HashMap<String, String>();
			expectMvDirMessage200.put("status", "200");

			System.out.println("the urlSrcDir   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			
			System.out.println("the urlDestDir   is : " + urlDestDir);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage200);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage404);
			tools.verifyResponse(setDeleteMethod(urlDelDestDir),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlDelDir_parent),
					expectDeleteMessage200);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ***mv dir 返回400 src dir 和dest dir 相同
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn400SrcDestSameTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_srcmv400";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = urlSrcDir;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage404 = new HashMap<String, String>();
			expectDeleteMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectDeleteMessage400 = new HashMap<String, String>();
			expectDeleteMessage400.put("status", "400");
			// set expect response message
			Map<String, String> expectMvDirMessage200 = new HashMap<String, String>();
			expectMvDirMessage200.put("status", "200");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectDeleteMessage400);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ***mv dir 返回401 移动到其他appid下 无权限
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn401DestAppidErrorTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_srcmv401";
		String destDir = "/dir_parent2/dir_dest/";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/2"
				+ "/1" + "/dir" + destDir + "?recursive=1";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			Map<String, String> expectMvDirMessage401 = new HashMap<String, String>();
			expectMvDirMessage401.put("status", "401");
			// set expect response message
			Map<String, String> expectMvDirMessage200 = new HashMap<String, String>();
			expectMvDirMessage200.put("status", "200");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage401);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * ***mv dir 返回404 src与dest appid不同 无权限
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn404SrcAppidErrorTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxB01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_src1";
		String destDir = "/dir_parent3/dir_dest/";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/2"
				+ "/1" + "/dir" + srcDir;
		tfsServer = tfs_NginxA01;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir + "?recursive=1";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			Map<String, String> expectMvDirMessage404 = new HashMap<String, String>();
			expectMvDirMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectMvDirMessage200 = new HashMap<String, String>();
			expectMvDirMessage200.put("status", "200");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage404);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv dir 返回403 将目录移动到子目录中
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn403PutSonTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_srcmv403";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = urlSrcDir + "/son_dir";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectDeleteMessage400 = new HashMap<String, String>();
			expectDeleteMessage400.put("status", "400");
			// set expect response message
			Map<String, String> expectMvDirMessage403 = new HashMap<String, String>();
			expectMvDirMessage403.put("status", "403");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage403);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv dir 返回404 src目录不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn404SrcDirNonExsitTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_src2";
		String destDir = "/dir_dest2";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectDeleteMessage400 = new HashMap<String, String>();
			expectDeleteMessage400.put("status", "400");
			// set expect response message
			Map<String, String> expectMvDirMessage404 = new HashMap<String, String>();
			expectMvDirMessage404.put("status", "404");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			// create dest dir		
			 tools.verifyResponse(setPostMethod(urlDestDir),
			 expectPostMessage);
			// mv to dest dir 
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage404);

			// tools.verifyResponse(setDeleteMethod(urlSrcDir),
			// expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlDestDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv dir 返回404 目标目录已存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn404DestDirExsitTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_src3";
		String destDir = "/dir_dest3";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectDeleteMessage400 = new HashMap<String, String>();
			expectDeleteMessage400.put("status", "400");
			// set expect response message
			Map<String, String> expectMvDirMessage404 = new HashMap<String, String>();
			expectMvDirMessage404.put("status", "404");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			// create src dir
			// create dest dir
			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			tools.verifyResponse(setPostMethod(urlDestDir), expectPostMessage);
			// mv to dest dir and the dest dir is already exist
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage404);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlDestDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv dir 返回404 目标父目录不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn404DestParentDirNonExsitTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcDir = "/dir_src4";
		String destDir = "/dir_destParent/dir_dest";
		String urlSrcDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + srcDir;
		String urlDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + destDir;

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectDeleteMessage404 = new HashMap<String, String>();
			expectDeleteMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDeleteMessage400 = new HashMap<String, String>();
			expectDeleteMessage400.put("status", "404");
			// set expect response message
			Map<String, String> expectMvDirMessage404 = new HashMap<String, String>();
			expectMvDirMessage404.put("status", "404");

			System.out.println("the postUrl   is : " + urlSrcDir);
			/* do post file action */
			System.out.println("test keypoint  is  begining");
			// create src dir

			tools.verifyResponse(setPostMethod(urlSrcDir), expectPostMessage);
			// do not creat parrent dir
			// tools.verifyResponse(setPostMethod(urlDestDir),
			// expectPostMessage);
			// mv to dest dir and the dest dir is already exist
			tools.verifyResponse(setMvDirPostMethod(urlDestDir, srcDir),
					expectMvDirMessage404);

			tools.verifyResponse(setDeleteMethod(urlSrcDir),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlDestDir),
					expectDeleteMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ls dir
	 */

	/*
	 * ls dir dir中有dir，验证是否正确
	 */
//bug 需要验证
	@Test
	public void test_TFS_RestfulUserDefinedLsDirReturn200DirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_200ls";
		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/1" + "/1" + "/dir" + "/dir_200ls";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);
			tools.verifyResponse(setPostMethod(postMetaUrl + "/name"),
					expectPostMessage);

			// GetMethod getMethod = setGetMethod(urlLs);
			JSONArray fileJasonArray = tfsServer.getFileMetaInfo("/dir_200ls");
			// 去掉 response中的"[]"并将内容转成restful返回的格式
			String body = tools.setJavaResposeJsonForRestful(tools
					.trimFirstAndLastChar(fileJasonArray.toString(), '[', ']'));
			// set expect response message
			Map<String, String> expectResponseJasonMessage = new HashMap<String, String>();
			expectResponseJasonMessage.put("status", "200");
			expectResponseJasonMessage.put("body", body);
			System.out.println("test to string is " + body);

			// 验证get dir 返回是否正确
			tools.verifyResponseWithJSONWithoutBeginEndChar(
					setGetMethod(urlLs), expectResponseJasonMessage);

			tools.verifyResponse(setGetMethod(postMetaUrl + "/name"),
					expectDeleteMessage);

			tools.verifyResponse(setGetMethod(postMetaUrl), expectDirMessage);

			// delete
			tools.verifyResponse(setDeleteMethod(postMetaUrl + "/name"),
					expectDeleteMessage);
			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}



	/*
	 * ls dir dir
	 * url错误
	 * 返回400错误
	 */

	@Test
	public void test_TFS_RestfulUserDefinedLsDirReturn400DirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
//		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
//				+ "/dir" + "/dir_201ls400";
		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/X" + "/1" + "/dir" + "/dir_201ls400";

		try { restartMeta();		

			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");			

			// set expect response message
			Map<String, String> expectMessage400 = new HashMap<String, String>();
			expectMessage400.put("status", "400");

			// 验证get dir 返回是否正确
			tools.verifyResponse(setGetMethod(urlLs), expectMessage400);

			// delete

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}
	
	
	
	
	@Test
	public void test_TFS_RestfulUserDefinedLsDirReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_201ls404";
		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/1" + "/1" + "/dir" + "/dir_201ls404";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");
			

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			

			// set expect response message
			Map<String, String> expectResponseJasonMessage = new HashMap<String, String>();
			expectResponseJasonMessage.put("status", "404");

			// 验证get dir 返回是否正确
			tools.verifyResponseWithJSONWithoutBeginEndChar(
					setGetMethod(urlLs), expectResponseJasonMessage);

			tools.verifyResponse(setGetMethod(postMetaUrl + "/name"),
					expectResponseJasonMessage);

			tools.verifyResponse(setGetMethod(postMetaUrl), expectResponseJasonMessage);

			// delete
			tools.verifyResponse(setDeleteMethod(postMetaUrl + "/name"),
					expectResponseJasonMessage);
			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectResponseJasonMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}
	
	
	
	@Test
	public void test_TFS_RestfulUserDefinedCheckDirReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/X" + "/1"
				+ "/dir" + "/dir_201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectHeadMessage400 = new HashMap<String, String>();
			expectHeadMessage400.put("status", "400");
			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");
			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");
			

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
		
			// 验证get dir 返回是否正确
			tools.verifyResponse(headMethod(postMetaUrl), expectHeadMessage400);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}
	
	
	/*
	 * head dir 检查指定是否存在目录，
	 * 访问目录不存在，
	 * 返回404 
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCheckDirReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_201check404";

		try { restartMeta();

	
			// set expect response message
			Map<String, String> expectHeadMessage404 = new HashMap<String, String>();
			expectHeadMessage404.put("status", "404");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
		
			// 验证get dir 返回是否正确
			tools.verifyResponse(headMethod(postMetaUrl), expectHeadMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}
	
	
	/*
	 * head dir，
	 * 返回200正确
	 */

	@Test
	public void test_TFS_RestfulUserDefinedHeadDirReturn200DirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_201head200";


		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");
			// set expect response message
			Map<String, String> expectHeadMessage200 = new HashMap<String, String>();
			expectHeadMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);
			tools.verifyResponse(setPostMethod(postMetaUrl + "/name"),
					expectPostMessage);
	
			

			// 验证get dir 返回是否正确
			tools.verifyResponse(headMethod(postMetaUrl), expectHeadMessage200);

			// delete
			tools.verifyResponse(setDeleteMethod(postMetaUrl + "/name"),
					expectDeleteMessage);
			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * head dir，
	 * 是文件，返回404 目录不存在
	 * 返回200正确
	 */

	@Test
	public void test_TFS_RestfulUserDefinedHeadDirReturn404DirTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlMeta = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_201head404";
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"+"/file"
				 + "/dir_201head404";
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"+"/file"
				+ "/dir_201head404" + "/name";


		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");
			// set expect response message
			Map<String, String> expectHeadMessage200 = new HashMap<String, String>();
			expectHeadMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectHeadMessage404 = new HashMap<String, String>();
			expectHeadMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			String postMetaUrl = urlMeta;
			System.out.println("the postUrl   is : " + postMetaUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postMetaUrl), expectPostMessage);
			tools.verifyResponse(setPostMethod(urlFile + "/name"+"?recursive=0"),
					expectPostMessage);
	
			

			// 验证get dir 返回是否正确
			tools.verifyResponse(headMethod(postMetaUrl+ "/name"), expectHeadMessage404);

			// delete
			tools.verifyResponse(setDeleteMethod(urlDelFile ),
					expectDeleteMessage);
			tools.verifyResponse(setDeleteMethod(postMetaUrl),
					expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}
	
}
