package com.taobao.gulu.tengine.test;


import java.util.HashMap;

import java.util.Map;


import net.sf.json.JSONArray;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.gulu.database.TFS;

import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

/* 自定义文件名测试
 * 
 * 
 */
public class TFS_Restful_UserDefinedFile_Test extends BaseCase 
{
	/*
	 * 
	 * 
	 * file 的creat delete get ls mv等
	 */

	/*
	 * create file 返回201
	 * 
	 */

	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn201Test() 
	{

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201"+methodName;
		String urlFile = urlDelFile + "?recursive=0";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

			tools.verifyResponse(setDeleteMethod(urlDelFile),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create file 返回401 appid不是appkey对应的appid
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn401Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/2" + "/1"
				+ "/file" + "/file_401";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "401");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create file 返回400 appid不合法
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/X" + "/1"
				+ "/file" + "/file_400";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "400");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create file 返回404 父目录不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;

		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/file" + "/dir_404/file_404?recursive=0";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "404");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * create dir 返回403 超过最大子目录数、文件数、目录深度
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreatefileReturn403Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url
				+ "v2/"
				+ tfsServer.getTfs_app_key()
				+ "/1"
				+ "/1"
				+ "/file"
				+ "/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403/dir_403?recursive=0";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "403");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * create file 返回409 文件已存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn409Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201"+methodName;
		String urlFile = urlDelFile + "?recursive=0";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectPostMessage409 = new HashMap<String, String>();
			expectPostMessage409.put("status", "409");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);
			tools.verifyResponse(setPostMethod(postFileUrl),
					expectPostMessage409);

			tools.verifyResponse(setDeleteMethod(urlDelFile),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete file
	 */

	/*
	 * delete file 返回200
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201"+methodName;
		String urlFile = urlDelFile + "?recursive=0";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expectPostMessage409 = new HashMap<String, String>();
			expectPostMessage409.put("status", "409");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setPostMethod(postFileUrl), expectPostMessage);

			tools.verifyResponse(setDeleteMethod(urlDelFile),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * delete file 返回400 目录/文件名不合规范
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeletefileReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/X" + "/1"
				+ "/file" + "/file_201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "400");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postFileUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete file 返回401 无权限（目前每个应用只对自己的appid下的空间拥有修改权限，包括创建/删除/移动目录/文件、写文件）
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteFileReturn401Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/2" + "/1"
				+ "/file" + "/dir_201";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "401");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postFileUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * delete file 返回404 文件不存在
	 */
	@Test
	public void test_TFS_RestfulUserDefinedDeleteFileReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/file" + "/dir_404";
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "404");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			tools.verifyResponse(setDeleteMethod(postFileUrl),
					expectDeleteMessage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv file 返回200 操作成功
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String srcFile = "/dir_src"+methodName;
		String destFile = "/dir_dest"+methodName;;
		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile + "?recursive=0";
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile + "?recursive=0";

		String urlDelSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;
		String urlDelDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile;

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

			System.out.println("the postUrl   is : " + urlSrcFile);
			/* do post file action */
			// create src file
			tools.verifyResponse(setPostMethod(urlSrcFile), expectPostMessage);
			// mv scr file
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvDirMessage200);
			// delete src file reponse 404
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectDeleteMessage404);
			// delete dest file response 200
			tools.verifyResponse(setDeleteMethod(urlDelDestFile),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv file 返回200 操作成功 creat parent dir
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn200CreatParentDirTest() {

		VerifyTool tools = new VerifyTool();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_dest"+methodName;

		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile + "?recursive=0";
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/paret_dir" + destFile + "?recursive=1";

		String urlDelSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;

		String urlDelDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/paret_dir" + destFile;

		String urlDelDestDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/dir" + "/paret_dir";

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

			System.out.println("the postUrl   is : " + urlSrcFile);
			/* do post file action */
			// create src file
			tools.verifyResponse(setPostMethod(urlSrcFile), expectPostMessage);
			// mv scr file
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvDirMessage200);
			// delete src file reponse 404
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectDeleteMessage404);
			// delete dest file response 200
			tools.verifyResponse(setDeleteMethod(urlDelDestFile),
					expectDeleteMessage200);
			tools.verifyResponse(setDeleteMethod(urlDelDestDir),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv file 返回400 src file 和dest file 相同
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn400SrcDestSameTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String url = NGINX.getRoot_url_adress();
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_src"+methodName;

		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile + "?recursive=0";
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile + "?recursive=0";
		String urlDelSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;

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
			Map<String, String> expectMvMessage200 = new HashMap<String, String>();
			expectMvMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectMvMessage400 = new HashMap<String, String>();
			expectMvMessage400.put("status", "400");

			System.out.println("the postUrl   is : " + urlSrcFile);
			/* do post file action */
			// create src file
			tools.verifyResponse(setPostMethod(urlSrcFile), expectPostMessage);
			// mv scr file
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvMessage400);
			// delete src file reponse 200
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * mv file 返回401 返回401 移动到其他appid下 无权限
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn401DestAppidErrorTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String methodName = "testfortestname";
		//Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String url = NGINX.getRoot_url_adress();
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_dest"+methodName;

		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile + "?recursive=0";
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/2"
				+ "/1" + "/file" + destFile + "?recursive=0";
		String urlDelSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;

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
			Map<String, String> expectMvMessage200 = new HashMap<String, String>();
			expectMvMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectMvMessage401 = new HashMap<String, String>();
			expectMvMessage401.put("status", "401");

			System.out.println("the postUrl   is : " + urlSrcFile);
			/* do post file action */
			// create src file
			tools.verifyResponse(setPostMethod(urlSrcFile), expectPostMessage);
			// mv scr file
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvMessage401);
			// delete src file reponse 200
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * mv dir 返回401 
	 * 移动到其他appid下 ,不支持移动到其他目录先。
	 * 报错，找不到目录
	 *  
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvDirReturn401SrcAppidErrorTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */

		TFS tfsServer = tfs_NginxB01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_dest"+methodName;

		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/2"
				+ "/1" + "/file" + srcFile + "?recursive=0";

		tfsServer = tfs_NginxA01;
		String url1 = NGINX.getRoot_url_adress();
		String urlDestFile = url1 + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile + "?recursive=0";

		tfsServer = tfs_NginxB01;
		String url2 = NGINX.getRoot_url_adress();
		String urlDelSrcFile = url2 + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;

		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");

			// set expect response message
			Map<String, String> expectMessage404 = new HashMap<String, String>();
			expectMessage404.put("status", "404");
			// set expect response message
			Map<String, String> expectDeleteMessage200 = new HashMap<String, String>();
			expectDeleteMessage200.put("status", "200");

			// set expect response message
			Map<String, String> expectMvMessage200 = new HashMap<String, String>();
			expectMvMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectMessage401 = new HashMap<String, String>();
			expectMessage401.put("status", "401");

			System.out.println("the postUrl   is : " + urlSrcFile);
			/* do post file action */
	
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMessage404);
			// delete src file reponse 200
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectMessage401);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv file 返回404 源文件不存在，返回404错误  存在bug修复后验证
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn404SrcNonExsitTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */

		TFS tfsServer = tfs_NginxA01;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String url = NGINX.getRoot_url_adress();
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_dest"+methodName;
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile;

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
			Map<String, String> expectMvMessage200 = new HashMap<String, String>();
			expectMvMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectMvMessage404 = new HashMap<String, String>();
			expectMvMessage404.put("status", "404");

			System.out.println("the urlDestFile   is : " + urlDestFile);
			/* do post file action */
			// mv scr file

			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * mv file 返回404 目标文件已存在
	 *  dosometihing bug修复后验证
	 */
	@Test
	public void test_TFS_RestfulUserDefinedMvFileReturn404DestFileExsitTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */

		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName(); 
		String srcFile = "/file_src"+methodName;
		String destFile = "/file_dest"+methodName;
		String urlDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile + "?recursive=0";
		String urlSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile + "?recursive=0";

		String urlDelDestFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + destFile;
		String urlDelSrcFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + srcFile;

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
			Map<String, String> expectMvMessage200 = new HashMap<String, String>();
			expectMvMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expectMvMessage404 = new HashMap<String, String>();
			expectMvMessage404.put("status", "404");

			System.out.println("the urlDestFile   is : " + urlDestFile);
			/* do post file action */
			// create scr file
			tools.verifyResponse(setPostMethod(urlSrcFile), expectPostMessage);
			// create dest file
			tools.verifyResponse(setPostMethod(urlDestFile), expectPostMessage);
			// mv src file dest file is already exist
			tools.verifyResponse(setMvFilePostMethod(urlDestFile, srcFile),
					expectMvMessage404);
			// delete scr file
			tools.verifyResponse(setDeleteMethod(urlDelSrcFile),
					expectDeleteMessage200);
			// delete dest file
			tools.verifyResponse(setDeleteMethod(urlDelDestFile),
					expectDeleteMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ls dir
	 */

	/*
	 * ls file dir中有file ，验证是否正确
	 */

	@Test
	public void test_TFS_RestfulUserDefinedLsFileReturn200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlDir = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/dir" + "/dir_forlsfile";
		String urlFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/file" + "/dir_forlsfile/lsfile";

		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/1" + "/1" + "/file" + "/dir_forlsfile/lsfile";

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

			System.out.println("the postUrl   is : " + urlFile);
			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			// creat dir and file
			tools.verifyResponse(setPostMethod(urlFile + "?recursive=1"),
					expectPostMessage);

			// 通过 tfs java客户端 ls 上面 创建 文件
			JSONArray fileJasonArray = tfsServer
					.getFileMetaFileInfo("/dir_forlsfile/lsfile");
			// 去掉 response中的"[]"并将内容转成restful返回的格式
			String body = tools.setJavaResposelsFileJsonForRestful(tools
					.trimFirstAndLastChar(fileJasonArray.toString(), '[', ']'));
			// set expect response message
			Map<String, String> expectResponseJasonMessage = new HashMap<String, String>();
			expectResponseJasonMessage.put("status", "200");
			expectResponseJasonMessage.put("body", body);
			System.out.println("test to string is " + body);

			// 验证ls file 返回是否正确
			tools.verifyResponseWithJSONWithoutBeginEndCharWhioutFileName(
					setGetMethod(urlLs), expectResponseJasonMessage);

			// delete 文件
			tools.verifyResponse(setDeleteMethod(urlFile), expectDeleteMessage);
			// delete 父目录
			tools.verifyResponse(setDeleteMethod(urlDir), expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * ls file 没有该文件，返回404
	 */

	@Test
	public void test_TFS_RestfulUserDefinedLsDirReturn404FileIsNotExistTest() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();

		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/1" + "/1" + "/file" + "/dir_forlsfile/lsfile";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectGetMessage404 = new HashMap<String, String>();
			expectGetMessage404.put("status", "404");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setGetMethod(urlLs), expectGetMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * ls file 目录不符合规范 返回400
	 */

	@Test
	public void test_TFS_RestfulUserDefinedLsDirReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();

		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/metadata"
				+ "/x" + "/1" + "/file" + "/dir_forlsfile/lsfile";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectGetMessage400 = new HashMap<String, String>();
			expectGetMessage400.put("status", "400");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setGetMethod(urlLs), expectGetMessage400);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * head file HEAD /v2/appkey/appid/uid/file/file_name HTTP/1.1 Host:
	 * 10.232.4.44:3900 Date: date 200 OK 文件存在 400 Bad Request 文件名不合规范 404 Not
	 * Found 文件不存在 500 Internal Server Error TFS或Nginx服务器内部错误 请求参数 无
	 */

	/*
	 * head file 文件名不符合规范 返回400
	 */

	@Test
	public void test_TFS_RestfulUserDefinedHeadDirReturn400Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();

		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/x" + "/1"
				+ "/file" + "/filename";

		try { restartMeta();

			// set expect response message
			Map<String, String> expectGetMessage400 = new HashMap<String, String>();
			expectGetMessage400.put("status", "400");

			// set expect response message
			Map<String, String> expectDeleteMessage = new HashMap<String, String>();
			expectDeleteMessage.put("status", "200");

			// set expect response message
			Map<String, String> expectDirMessage = new HashMap<String, String>();
			expectDirMessage.put("status", "200");

			System.out.println("the urlLs   is : " + urlLs);
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(headMethod(urlLs), expectGetMessage400);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * head dir， 文件找不到 返回404正确
	 */

	@Test
	public void test_TFS_RestfulUserDefinedHeadFileReturn404Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlLs = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1"
				+ "/file" + "/file_non_exist";

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
			Map<String, String> expectGetMessage404 = new HashMap<String, String>();
			expectGetMessage404.put("status", "404");

			/* do post file action */
			tools.verifyResponse(headMethod(urlLs), expectGetMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	/*
	 * head file， 是文件，返回返回200
	 */

	@Test
	public void test_TFS_RestfulUserDefinedHeadFileReturn200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();

		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_for_head_200";
		String urlHeadFile = urlDelFile;
		String urlFile = urlDelFile + "?recursive=0";

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

			System.out.println("the urlFile   is : " + urlFile);
			/* do post file action */
			System.out.println("creat fiel  begin");
			tools.verifyResponse(setPostMethod(urlFile), expectPostMessage);
			tools.verifyResponse(headMethod(urlHeadFile), expectHeadMessage200);

			// delete
			tools.verifyResponse(setDeleteMethod(urlDelFile),
					expectDeleteMessage);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * write file
	 * 
	 * @param int offset
	 * 
	 * @param int size
	 * 
	 * @return
	 */

	public void test_TFS_RestfulUserDefinedWriteFileReturn200Test(int offset, int size,String methodName)
	{

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress(); 
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/"+ methodName;
		String urlFile = urlDelFile + "?offset=" + offset + "&size=" + size;
		String localFile = _2kFile;
		try 
		{

			// set expect response message
			Map<String, String> expectPostMessage201 = new HashMap<String, String>();
			expectPostMessage201.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			System.out.println("the urlDelFile   is : " + urlDelFile);
			/* do post file action */
			System.out.println("creat dir  begin");

			// create file
			tools.verifyResponse(setDeleteMethod(urlDelFile));
			
			tools.verifyResponse(setPostMethod(urlDelFile),expectPostMessage201);
			// write file
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),expecMessage200);

			tools.verifyResponse(setDeleteMethod(urlDelFile), expecMessage200);
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
		
	}

	/*
	 * write file， offset 0 size 0 返回返回200
	 */

	@Test
	public void test_TFS_RestfulUserDefinedWriteFileReturn200Offset0Size0Test() {

		int offset = 0;
		int size = 0;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName()+"5";
		test_TFS_RestfulUserDefinedWriteFileReturn200Test(offset, size, methodName);
	}

	public void test_TFS_RestfulUserDefinedWriteFileReturn200Offset0Size2000Test() {

		int offset = 0;
		int size = 2000;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		test_TFS_RestfulUserDefinedWriteFileReturn200Test(offset, size,methodName);
	}

	public void test_TFS_RestfulUserDefinedWriteFileReturn200Offset4000Size2049Test() {

		int offset = 4000;
		int size = 2049;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		test_TFS_RestfulUserDefinedWriteFileReturn200Test(offset, size,methodName);
	}

	public void test_TFS_RestfulUserDefinedWriteFileReturn200Offset200Size200Test() {

		int offset = 200;
		int size = 200;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		test_TFS_RestfulUserDefinedWriteFileReturn200Test(offset, size,methodName);
	}

	@Test
	public void test_TFS_RestfulUserDefinedWtiteFileReturn400Offset0Size0Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/X"
				+ "/1" + "/file" + "/file_201";
		String urlFile = urlDelFile + "?offset=0&size=0";
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expecMessage400 = new HashMap<String, String>();
			expecMessage400.put("status", "400");
			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			// write file return 400
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage400);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	// return 401 无权限
	@Test
	public void test_TFS_RestfulUserDefinedWriteFileReturn401Offset20Size200Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/2"
				+ "/1" + "/file" + "/file_201";
		String urlFile = urlDelFile + "?offset=0&size=0";
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expecMessage401 = new HashMap<String, String>();
			expecMessage401.put("status", "401");
			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			// write file return 401
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage401);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		}
	}

	// return 404 文件不存在
	@Test
	public void test_TFS_RestfulUserDefinedWriteFileReturn404Offset0Size0Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		String url = NGINX.getRoot_url_adress();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201"+methodName;
		String urlFile = urlDelFile + "?offset=0&size=0";
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage = new HashMap<String, String>();
			expectPostMessage.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage404 = new HashMap<String, String>();
			expecMessage404.put("status", "404");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			// write file
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage404);

			tools.verifyResponse(setDeleteMethod(urlDelFile), expecMessage404);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	// return 409 写偏移已存在数据
	public void test_TFS_RestfulUserDefinedWriteFileReturn409Test() {

		VerifyTool tools = new VerifyTool();
		int offset = 20;
		int size = 50;
		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		String url = NGINX.getRoot_url_adress();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201"+methodName;
		String urlFile = urlDelFile + "?offset=" + offset + "&size=" + size;
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage201 = new HashMap<String, String>();
			expectPostMessage201.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expecMessage409 = new HashMap<String, String>();
			expecMessage409.put("status", "409");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			String getFileUrl = urlFile;
			System.out.println("the getUrl   is : " + getFileUrl);
			/* do post file action */
			System.out.println("creat dir  begin");

			// create file
			tools.verifyResponse(setPostMethod(urlDelFile),
					expectPostMessage201);
			// write file
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage200);

			// write file again 返回409
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage409);

			tools.verifyResponse(setDeleteMethod(urlDelFile), expecMessage200);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	/*
	 * read file
	 * 
	 * @param int offset
	 * 
	 * @param int size
	 * 
	 * @return
	 */


	public void test_TFS_RestfulUserDefinedReadFileReturn200Test(int offset,
			int size) {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201";
		String urlFile = urlDelFile + "?offset=" + offset + "&size=" + size;
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage201 = new HashMap<String, String>();
			expectPostMessage201.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			/* do post file action */
			System.out.println("key point  begin");

			// create file
			tools.verifyResponse(setPostMethod(urlDelFile),
					expectPostMessage201);
			// write file
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),
					expecMessage200);

			// read file
			tools.verifyResponse(setGetMethod(postFileUrl), expecMessage200);

			tools.verifyResponse(setDeleteMethod(urlDelFile), expecMessage200);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

	public void test_TFS_RestfulUserDefinedReadFileReturn200Offset200Size200Test() {

		int offset = 200;
		int size = 200;
		test_TFS_RestfulUserDefinedReadFileReturn200Test(offset, size);
	}

	public void test_TFS_RestfulUserDefinedReadFileReturn200Offset0Size0Test() {

		int offset = 0;
		int size = 0;
		test_TFS_RestfulUserDefinedReadFileReturn200Test(offset, size);
	}

	public void test_TFS_RestfulUserDefinedReadFileReturn200Offset2049Size2049Test() {

		int offset = 2049;
		int size = 2049;
		test_TFS_RestfulUserDefinedReadFileReturn200Test(offset, size);
	}

	@Test
	public void test_TFS_RestfulUserDefinedReadFileReturn400Test() {

		VerifyTool tools = new VerifyTool();
		int offset = 20;
		int size = 200;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1" + "/1" + "/file" + "/file_201";
		
		String urlFile = urlDelFile + "?offset=" + offset + "&size=" + size;
		
		String urlReadFile = url + "v2/" + tfsServer.getTfs_app_key() + "/x" + "/1" + "/file" + "/file_201";
		
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage201 = new HashMap<String, String>();
			expectPostMessage201.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expecMessage400 = new HashMap<String, String>();
			expecMessage400.put("status", "400");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			/* do post file action */
			System.out.println("key point  begin");

			// create file
			tools.verifyResponse(setDeleteMethod(urlDelFile));
			
			tools.verifyResponse(setPostMethod(urlDelFile),expectPostMessage201);
			// write file
			tools.verifyResponse(setPutMethod(postFileUrl, localFile),expecMessage200);

			// read file
			tools.verifyResponse(setGetMethod(urlReadFile), expecMessage400);
			//delete  file
			tools.verifyResponse(setDeleteMethod(urlDelFile), expecMessage200);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}
	
	@Test
	public void test_TFS_RestfulUserDefinedReadFileReturn404Test() {

		VerifyTool tools = new VerifyTool();
		int offset = 20;
		int size = 200;

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = NGINX.getRoot_url_adress();
		String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
		String urlDelFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_2011"+methodName;
		String urlFile = urlDelFile + "?offset=" + offset + "&size=" + size;
		String urlReadFile = url + "v2/" + tfsServer.getTfs_app_key() + "/1"
				+ "/1" + "/file" + "/file_201xx"+methodName;
		String localFile = _2kFile;
		try { restartMeta();

			// set expect response message
			Map<String, String> expectPostMessage201 = new HashMap<String, String>();
			expectPostMessage201.put("status", "201");
			// set expect response message
			Map<String, String> expecMessage200 = new HashMap<String, String>();
			expecMessage200.put("status", "200");
			// set expect response message
			Map<String, String> expecMessage404 = new HashMap<String, String>();
			expecMessage404.put("status", "404");

			String postFileUrl = urlFile;
			System.out.println("the postUrl   is : " + postFileUrl);
			/* do post file action */
			System.out.println("key point  begin");
			// read file
			tools.verifyResponse(setGetMethod(urlReadFile), expecMessage404);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); Assert.assertTrue(false);
		} 
	}

}
