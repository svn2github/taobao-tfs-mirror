package com.taobao.gulu.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.gulu.database.TFS;
import com.taobao.gulu.help.proc.HelpProc;
import com.taobao.gulu.server.ShellServer;

/**
 * @author gongyuan.cz
 * 
 */

public class VerifyTool extends HelpProc {

	// ------------------------ MD5 ------------------------------
	protected static char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	// ------------------------ MD5 ------------------------------

	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 */
	public void verifyResponse(HttpMethod method,Map<String, String> expectMessage) throws HttpException,IOException 
	{
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) 
		{
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) 
			{
				String responseBody = method.getResponseBodyAsString();
				System.out.println("value is" + value);
				System.out.println("response is" + responseBody);
				Assert.assertEquals("Response body not equal to expect !!",
						true, responseBody.contains(value));
			} 
			else if (key.equals("status")) 
			{
				// value是前面设定的期望值，response是请求后返回值
				System.out.println(Integer.parseInt(value));
				System.out.println(status);
				Assert.assertEquals("Response status code not equal to expect !!",Integer.parseInt(value), status);
			} 
			else 
			{
				if (value == null) 
				{
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key));
				}
				else 
				{
					Assert.assertNotNull("Response header " + key + "is null !",method.getResponseHeader(key));
					Assert.assertEquals("Response header " + key + "not equal to expect !", true, method.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	public void verifyResponse(HttpMethod method) throws HttpException,IOException 
	{
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(300000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);
		System.out.println("the status: "+status);
		
		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}
	
	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 */
	public String verifyResponseAndGetTFSFileName(HttpMethod method, Map<String, String> expectMessage) throws HttpException,IOException, JSONException 
	{
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);
		System.out.println("the status: "+status);
		
		String responseBody = method.getResponseBodyAsString();
		System.out.println(responseBody);

		JSONObject jsonResponse = new JSONObject(responseBody);
		String tfsFileName = jsonResponse.getString("TFS_FILE_NAME");

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext())
		{
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("status"))
			{
				Assert.assertEquals("Response status code not equal to expect !!", status,Integer.parseInt(value));
			} 
			else 
			{
				if (value == null) 
				{
					Assert.assertNull("Response header " + key + "not null!",method.getResponseHeader(key));
				} 
				else 
				{
					Assert.assertNotNull("Response header " + key + "is null !",method.getResponseHeader(key));
					Assert.assertEquals("Response header " + key + "not equal to expect !", true, method.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
		return tfsFileName;
	}

	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 */
	public void verifyResponseWithJSON(HttpMethod method,
			Map<String, String> expectMessage) throws HttpException,
			IOException, JSONException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				String responseBody = method.getResponseBodyAsString();
				System.out.print(responseBody);

				JSONObject jsonResponse = new JSONObject(responseBody);
				// remove time info
				jsonResponse.remove("FILE_NAME");
				jsonResponse.remove("MODIFY_TIME");
				jsonResponse.remove("CREATE_TIME");
				jsonResponse.remove("CRC");

				JSONObject jsonExpect = new JSONObject(value);

				Assert.assertEquals("Response body not equal to expect !!",
						true,
						jsonResponse.toString().equals(jsonExpect.toString()));
			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value == null) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key));
				} else {
					Assert.assertNotNull(
							"Response header " + key + "is null !",
							method.getResponseHeader(key));
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 */
	public void verifyResponseWithJSONWithoutBeginEndChar(HttpMethod method,
			Map<String, String> expectMessage) throws HttpException,
			IOException, JSONException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				// 去除response body中的[],
				String responseBody = trimFirstAndLastChar(
						method.getResponseBodyAsString(), '[', ']');
				System.out.println(responseBody);
				System.out.println(value);
				JSONObject jsonResponse = new JSONObject(responseBody);
				// remove time info
				jsonResponse.remove("IS_FILE");
				jsonResponse.remove("MODIFY_TIME");
				jsonResponse.remove("CREATE_TIME");
				// jsonResponse.remove("CRC");

				JSONObject jsonExpect = new JSONObject(value);

				Assert.assertEquals("Response body not equal to expect !!",
						true,
						jsonResponse.toString().equals(jsonExpect.toString()));
			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value == null) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key));
				} else {
					Assert.assertNotNull(
							"Response header " + key + "is null !",
							method.getResponseHeader(key));
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}
	public void verifyResponseWithJSONWithoutBeginEndCharWhioutFileName(HttpMethod method,
			Map<String, String> expectMessage) throws HttpException,
			IOException, JSONException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				// 去除response body中的[],
				String responseBody = trimFirstAndLastChar(
						method.getResponseBodyAsString(), '[', ']');
				System.out.println(responseBody);
				System.out.println(value);
				JSONObject jsonResponse = new JSONObject(responseBody);
				// remove time info
				jsonResponse.remove("IS_FILE");
				jsonResponse.remove("MODIFY_TIME");
				jsonResponse.remove("CREATE_TIME");
				jsonResponse.remove("NAME");
				// jsonResponse.remove("CRC");

				JSONObject jsonExpect = new JSONObject(value);

				Assert.assertEquals("Response body not equal to expect !!",
						true,
						jsonResponse.toString().equals(jsonExpect.toString()));
			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value == null) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key));
				} else {
					Assert.assertNotNull(
							"Response header " + key + "is null !",
							method.getResponseHeader(key));
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}
	
	public String converttfsFileNametoJson(TFS tfsServer, String tfsFileName,
			String tfsSuffix) throws TfsException, ParseException 
			{

		FileInfo fileInfo = tfsServer.getTfsStat(tfsFileName, tfsSuffix);
		if (fileInfo == null) {
			System.out.println("fileinfo is null ");
			return null;
		}
		int blockID = tfsServer.getTfsBlockID(tfsFileName, tfsSuffix);
		return "{"
				// + "\"file name\": \"" + tfsFileName + "\","
				+ "\"BLOCK_ID\": " + blockID + ",\"FILE_ID\": "
				+ fileInfo.getId() + ",\"OFFSET\": " + fileInfo.getOffset()
				+ ",\"SIZE\": " + fileInfo.getLength() + ",\"OCCUPY_SIZE\": "
				+ fileInfo.getOccupyLength()
				// + ",\"MODIFY_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getModifyTime()) + "\""
				// + ",\"CREATE_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getCreateTime()) + "\""
				+ ",\"STATUS\": " + fileInfo.getFlag()
				// + ",\"CRC\": " + fileInfo.getCrc()
				+ "}";
	}

	public void verifyTFSFileStatu(TFS tfsServer, String tfsFileName,String tfsSuffix, int expectStatu) throws TfsException,ParseException 
	{
		FileInfo fileInfo = tfsServer.getTfsStat(tfsFileName, tfsSuffix);
		Assert.assertEquals("TFS File status not equals to expect!",expectStatu, fileInfo.getFlag());
	}

	public void verifyTFSFileStatu(TFS tfsServer, String tfsFileName,
			int expectStatu) throws TfsException, ParseException {
		FileInfo fileInfo = tfsServer.getTfsStat(tfsFileName);

		Assert.assertEquals("TFS File status not equals to expect!",
				expectStatu, fileInfo.getFlag());
	}

	public String converttfsFileNametoJson(TFS tfsServer, String tfsFileName)
			throws TfsException, ParseException {
		FileInfo fileInfo = tfsServer.getTfsStat(tfsFileName);
		int blockID = tfsServer.getTfsBlockID(tfsFileName);

		return "{"

				// + "\"file name\": \"" + tfsFileName + "\","
				+ "\"BLOCK_ID\": " + blockID + ",\"FILE_ID\": "
				+ fileInfo.getId() + ",\"OFFSET\": " + fileInfo.getOffset()
				+ ",\"SIZE\": " + fileInfo.getLength() + ",\"OCCUPY_SIZE\": "
				+ fileInfo.getOccupyLength()
				// + ",\"MODIFY_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getModifyTime()) + "\""
				// + ",\"CREATE_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getCreateTime()) + "\""
				+ ",\"STATUS\": " + fileInfo.getFlag()
				// + ",\"CRC\": " + fileInfo.getCrc()
				+ "}";
	}

	public String converFileMetaInfotoJson(FileMetaInfo fileMetaInfo)
			throws TfsException, ParseException {

		return "{"
				// [fileName: name fileType: dir pid: 666539 id: 666540
				// createTime: 2012-09-13 17:05:40 modifyTime: 2012-09-13
				// 17:05:40 length: 0 version: 0]

				+ "\"NAME\": \"" + fileMetaInfo.getFileName() + "\","
				+ "\"PID\": " + fileMetaInfo.getPid() + ",\"SIZE\": "
				+ fileMetaInfo.getLength() + ",\"IS_FILE\": "
				+ fileMetaInfo.isFile() + ",\"VER_NO\": "
				+ fileMetaInfo.getVersion()
				// + ",\"MODIFY_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getModifyTime()) + "\""
				// + ",\"CREATE_TIME\": \"" +
				// TfsUtil.timeToString(fileInfo.getCreateTime()) + "\""
				// + ",\"STATUS\": " + fileInfo.getFlag()
				// + ",\"CRC\": " + fileInfo.getCrc()
				+ "}";
	}

	public void verifyResponseWithCurl(ShellServer server, String header,
			String url, String expectHeader, String expectResponse)
			throws Exception {

		String cmd = "curl -i " + "\"" + url + "\"" + " --header " + "\""
				+ header + "\"";
		executeRemoteCommand(server, cmd, expectHeader, "");
		executeRemoteCommand(server, cmd, expectResponse, "");
	}

	public void verifyResponseWithCurl(ShellServer server, String header,
			String cookie, String url, String expectHeader,
			String expectResponse) throws Exception {

		String cmd = "curl -i " + "\"" + url + "\"" + " --header " + "\""
				+ header + "\"" + " --cookie " + "\"" + cookie + "\"";
		executeRemoteCommand(server, cmd, expectHeader, "");
		executeRemoteCommand(server, cmd, expectResponse, "");
	}

	// public void verifyResponseWithCurl(ShellServer server, String data,
	// String url, String expectResponse) throws Exception {
	//
	// String cmd = "curl -i " + "\"" + url + "\"" + " --data " + "\'" + data +
	// "\'";
	// executeRemoteCommand(server,cmd,expectResponse,"");
	// }

	public void verifyResponseWithCurl(ShellServer server, String url,
			String expectResponse) throws Exception {

		String cmd = "curl -i " + "\"" + url + "\"";
		executeRemoteCommand(server, cmd, expectResponse, "");
	}

	public void verifyResponseWithCMD(ShellServer server, String cmd,
			String expectHeader, String expectResponse) throws Exception {
		executeRemoteCommand(server, cmd, expectHeader, "");
		executeRemoteCommand(server, cmd, expectResponse, "");
	}

	public void verifyResponseWithCMDNotEQ(ShellServer server, String cmd,
			String expectResponse) throws Exception {
		executeRemoteCommandNotEQ(server, cmd, expectResponse);
	}

	public void verifyResponseHeaderWithCMD(ShellServer server, String cmd,
			String expectHeader) throws Exception {
		executeRemoteCommand(server, cmd, expectHeader, "");
	}

	public void verifyResponseHeaderWithCMDNotEQ(ShellServer server,
			String cmd, String expectHeader) throws Exception {
		executeRemoteCommandNotEQ(server, cmd, expectHeader);
	}

	public void verifyResponseWithCMD(ShellServer server, String cmd,
			String expectResponse) throws Exception {
		executeRemoteCommand(server, cmd, expectResponse, "");
	}

	public void verifyCMD(ShellServer server, String cmd, String expectHeader,
			String expectResponse) throws Exception {
		executeRemoteCommand(server, cmd, expectHeader, expectResponse);
	}

	public void verifyCMDWithDumbPTY(ShellServer server, String cmd,
			String expectHeader, String expectResponse) throws Exception {
		executeRemoteCommandWithDumbPTY(server, cmd, expectHeader,
				expectResponse);
	}

	public void verifyCMDNotEQ(ShellServer server, String cmd,
			String expectHeader, String expectResponse) throws Exception {
		executeRemoteCommandNotEQ(server, cmd, expectHeader);
	}

	/*
	 * map --> key:body value: local file path file size should not large than
	 * byte[60 * 1024 * 1024]
	 */
	public void verifyResponseBodyWithLocalFile(HttpMethod method,Map<String, String> expectMessage) throws HttpException,IOException 
	{
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) 
		{
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) 
			{
				InputStream localFileInputStream = new FileInputStream(value);
				InputStream ResponseFileInputStream = method.getResponseBodyAsStream();

				byte[] localFileDataArray = new byte[60*1024*1024];
				byte[] responseFileDataArray = new byte[60*1024*1024];
				int localFileReadCnt = 0;
				int responseFileReadCnt = 0;
				// read response file
				while (true)
				{
					int tmp = ResponseFileInputStream.read(responseFileDataArray, responseFileReadCnt,responseFileDataArray.length - responseFileReadCnt);
					if (tmp == -1) 
					{
						break;
					}
					responseFileReadCnt += tmp;
				}
				// read local file
				while (true) 
				{
					int tmp = localFileInputStream.read(localFileDataArray,localFileReadCnt, localFileDataArray.length- localFileReadCnt);
					if (tmp == -1) 
					{
						break;
					}
					localFileReadCnt += tmp;
				}

				System.out.println("responseFileReadCnt is : "+ responseFileReadCnt);
				System.out.println("localFileReadCnt is : " + localFileReadCnt);

				// test the response file size equals to local file size
				Assert.assertEquals("local file size is not equals to response file size !",localFileReadCnt, responseFileReadCnt);

				for (int i = 0; i < localFileReadCnt; i++) 
				{
					Assert.assertEquals("local file not equals to response file size",localFileDataArray[i], responseFileDataArray[i]);
				}

			} 
			else if (key.equals("status")) 
			{
				Assert.assertEquals("Response status code not equal to expect !!", status,Integer.parseInt(value));
			} 
			else 
			{
				if (value.isEmpty()) {Assert.assertNull("Response header " + key + "not null!",method.getResponseHeader(key).getValue());
			} 
				else 
				{
					Assert.assertEquals("Response header " + key + "not equal to expect !", true, method.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:body value: local file path file size should not large than
	 * byte[60 * 1024 * 1024] offset is for local file
	 */
	public void verifyResponseBodyWithLocalFile(HttpMethod method,
			Map<String, String> expectMessage, int offset)
			throws HttpException, IOException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				InputStream localFileInputStream = new FileInputStream(value);
				InputStream ResponseFileInputStream = method
						.getResponseBodyAsStream();

				byte[] localFileDataArray = new byte[60 * 1024 * 1024];
				byte[] responseFileDataArray = new byte[60 * 1024 * 1024];
				int localFileReadCnt = 0;
				int responseFileReadCnt = 0;
				// read response file
				while (true) {
					int tmp = ResponseFileInputStream.read(
							responseFileDataArray, responseFileReadCnt,
							responseFileDataArray.length - responseFileReadCnt);
					if (tmp == -1) {
						break;
					}
					responseFileReadCnt += tmp;
				}

				// read local file in offset sizeco
				// for (int i = 0; i < offset; i++) {
				// if (localFileInputStream.read() == -1) {
				// break;
				// }
				// }
				// read local file
				while (true) {
					int tmp = localFileInputStream.read(localFileDataArray,
							localFileReadCnt, localFileDataArray.length
									- localFileReadCnt);
					if (tmp == -1) {
						break;
					}
					localFileReadCnt += tmp;
				}

				// test the response file size equals to local file size
				Assert.assertEquals(
						"local file size is not equals to response file size !",
						localFileReadCnt - offset, responseFileReadCnt);

				for (int i = 0; i < responseFileReadCnt; i++) {
					Assert.assertEquals(
							"local file not equals to response file!",
							localFileDataArray[i + offset],
							responseFileDataArray[i]);
				}

			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value.isEmpty()) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key).getValue());
				} else {
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:body value: local file path file size should not large than
	 * byte[60 * 1024 * 1024] offset is for local file
	 */
	public void verifyResponseBodyWithLocalFile(HttpMethod method,
			Map<String, String> expectMessage, int offset, int size)
			throws HttpException, IOException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				InputStream localFileInputStream = new FileInputStream(value);
				InputStream ResponseFileInputStream = method
						.getResponseBodyAsStream();

				byte[] localFileDataArray = new byte[60 * 1024 * 1024];
				byte[] responseFileDataArray = new byte[60 * 1024 * 1024];
				int localFileReadCnt = 0;
				int responseFileReadCnt = 0;
				// read response file
				while (true) {
					int tmp = ResponseFileInputStream.read(
							responseFileDataArray, responseFileReadCnt,
							responseFileDataArray.length - responseFileReadCnt);
					if (tmp == -1) {
						break;
					}
					responseFileReadCnt += tmp;
				}

				// read local file in offset size
				// for (int i = 0; i < offset; i++) {
				// if (localFileInputStream.read() == -1) {
				// break;
				// }
				// }
				// read local file
				while (true) {
					int tmp = localFileInputStream.read(localFileDataArray,
							localFileReadCnt, localFileDataArray.length
									- localFileReadCnt);
					if (tmp == -1) {
						break;
					}
					localFileReadCnt += tmp;
				}

				// test the response file size equals to local file size
				Assert.assertEquals(
						"local file size is not equals to response file size !",
						size, responseFileReadCnt);

				for (int i = 0; i < size; i++) {
					Assert.assertEquals(
							"local file not equals to response file!",
							localFileDataArray[i + offset],
							responseFileDataArray[i]);
				}

			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value.isEmpty()) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key).getValue());
				} else {
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 * 
	 * @param SA_IP: source address IP
	 * 
	 * @param SA_Port: source address port
	 * 
	 * @param DAD_IP: destination address IP
	 * 
	 * @param DAD_Port: destination address port
	 */
	public void verifyResponseInIpBinding(String SA_IP, int SA_Port,
			String DAD_IP, int DAD_Port, String filename,
			Map<String, String> requestHeader,
			Map<String, String> expectResponseMessage) throws HttpException,
			IOException {

		Socket s = new Socket();
		s.bind(new InetSocketAddress(SA_IP, SA_Port));
		s.connect(new InetSocketAddress(DAD_IP, DAD_Port));

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				s.getOutputStream()));

		Set<String> requestHeaderKeySet = requestHeader.keySet();
		Iterator<String> requestHeaderIt = requestHeaderKeySet.iterator();
		String url = "GET /%s HTTP/1.1\r\nHost:" + DAD_IP;
		while (requestHeaderIt.hasNext()) {
			String key = (String) requestHeaderIt.next();
			String value = (String) requestHeader.get(key);
			url = url + "\r\n" + key + ":" + value;
		}
		url = url + "\r\n\r\n";

		String request = String.format(url, filename);
		writer.write(request);
		writer.flush();

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				s.getInputStream()));
		String line = reader.readLine();
		StringBuffer sb = new StringBuffer();
		while (line != null) {
			sb.append(line);
			line = reader.readLine();
		}

		Set<String> responseMessageKeySet = expectResponseMessage.keySet();
		Iterator<String> responseMessageIt = responseMessageKeySet.iterator();

		while (responseMessageIt.hasNext()) {
			String key = (String) responseMessageIt.next();
			String value = (String) expectResponseMessage.get(key);
			if (key.equals("status")) {
				Assert.assertEquals("response not equals to expect!", true, sb
						.toString().contains(value));
			} else {
				Assert.assertEquals("response not equals to expect!", true, sb
						.toString().contains(key + ": " + value));
			}
		}

		reader.close();
		writer.close();
		s.close();
	}

	/*
	 * map --> key:status value: status code map --> key:body value: response
	 * body
	 * 
	 * @param SA_IP: source address IP
	 * 
	 * @param SA_Port: source address port
	 * 
	 * @param DAD_IP: destination address IP
	 * 
	 * @param DAD_Port: destination address port
	 */
	public void verifyResponseInIpBinding(String SA_IP, int SA_Port,
			String DAD_IP, int DAD_Port, String path,
			String expectResponseMessage) throws HttpException, IOException {

		Socket s = new Socket();
		s.bind(new InetSocketAddress(SA_IP, SA_Port));
		s.connect(new InetSocketAddress(DAD_IP, DAD_Port));

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				s.getOutputStream()));

		String url = "GET /%s HTTP/1.1\r\nHost:" + DAD_IP;
		url = url + "\r\n\r\n";

		String request = String.format(url, path);
		writer.write(request);
		writer.flush();

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				s.getInputStream()));
		String line = reader.readLine();
		StringBuffer sb = new StringBuffer();
		while (line != null) {
			sb.append(line);
			line = reader.readLine();
		}

		Assert.assertEquals("response not equals to expect!", true, sb
				.toString().contains(expectResponseMessage));

		reader.close();
		writer.close();
		s.close();
	}

	public void showResponse(HttpMethod method) throws HttpException,
			IOException {
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		System.out.println("Response Header: ---------------> ");
		System.out.println("Status: " + status);

		Header[] headerArray = method.getResponseHeaders();
		for (Header h : headerArray) {
			System.out.println(h.getName() + ": " + h.getValue());
		}
		System.out.println();
		System.out.println("Response Body: ---------------> ");
		System.out.println(method.getResponseBodyAsString());

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	public void showResponseInIpBinding(String SA_IP, int SA_Port,
			String DAD_IP, int DAD_Port, String filename,
			Map<String, String> requestHeader) throws HttpException,
			IOException {

		Socket s = new Socket();
		s.bind(new InetSocketAddress(SA_IP, SA_Port));
		s.connect(new InetSocketAddress(DAD_IP, DAD_Port));

		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
				s.getOutputStream()));

		Set<String> requestHeaderKeySet = requestHeader.keySet();
		Iterator<String> requestHeaderIt = requestHeaderKeySet.iterator();
		String url = "GET /%s HTTP/1.1\r\nHost:" + DAD_IP;
		while (requestHeaderIt.hasNext()) {
			String key = (String) requestHeaderIt.next();
			String value = (String) requestHeader.get(key);
			url = url + "\r\n" + key + ":" + value;
		}
		url = url + "\r\n\r\n";

		String request = String.format(url, filename);
		writer.write(request);
		writer.flush();

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				s.getInputStream()));
		String line = reader.readLine();
		while (line != null) {
			System.out.println(line);
			line = reader.readLine();
		}

		reader.close();
		writer.close();
		s.close();
	}

	// ----------------------- Use MD5 to Verify --------------------------

	// ------ MD5 help method --------------
	private static String bufferToHex(byte bytes[]) {
		return bufferToHex(bytes, 0, bytes.length);
	}

	private static String bufferToHex(byte bytes[], int m, int n) {
		StringBuffer stringbuffer = new StringBuffer(2 * n);
		int k = m + n;
		for (int l = m; l < k; l++) {
			appendHexPair(bytes[l], stringbuffer);
		}
		return stringbuffer.toString();
	}

	private static void appendHexPair(byte bt, StringBuffer stringbuffer) {
		char c0 = hexDigits[(bt & 0xf0) >> 4];
		char c1 = hexDigits[bt & 0xf];
		stringbuffer.append(c0);
		stringbuffer.append(c1);
	}

	// ------ MD5 help method ---------------

	/*
	 * convert String to MD5
	 */
	protected String stringToMD5(String string) {
		MessageDigest messagedigest = null;

		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println(VerifyTool.class.getName()
					+ "initialize MD5Util fail!");
			nsaex.printStackTrace();
		}

		// reset messagedigest
		messagedigest.reset();
		messagedigest.update(string.getBytes());
		return bufferToHex(messagedigest.digest());
	}

	public void verifyResponseBodyWithMD5(HttpMethod method,
			Map<String, String> expectMessage, String expectResponseBodyMD5Code)
			throws IOException {

		MessageDigest messagedigest = null;

		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println(VerifyTool.class.getName()
					+ "initialize MD5Util fail!");
			nsaex.printStackTrace();
		}

		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				InputStream ResponseFileInputStream = method
						.getResponseBodyAsStream();

				byte[] DataArray = new byte[60 * 1024 * 1024];
				int tmp = 0;

				// reset messagedigest
				messagedigest.reset();

				// read response file
				while (true) {
					tmp = ResponseFileInputStream.read(DataArray, 0,
							DataArray.length);
					if (tmp == -1) {
						break;
					}
					messagedigest.update(DataArray, 0, tmp);
				}
				String responseFileMD5Code = bufferToHex(messagedigest.digest());

				Assert.assertEquals("local file not equals to response file!",
						responseFileMD5Code, expectResponseBodyMD5Code);

			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!",
						Integer.parseInt(value), status);
			} else {
				if (value.isEmpty()) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key).getValue());
				} else {
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:body value: local file path file size should not large than
	 * byte[60 * 1024 * 1024] use MD5 to verify
	 */
	public void verifyResponseBodyWithLocalFileInMD5(HttpMethod method,
			Map<String, String> expectMessage) throws HttpException,
			IOException {

		MessageDigest messagedigest = null;

		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println(VerifyTool.class.getName()
					+ "initialize MD5Util fail!");
			nsaex.printStackTrace();
		}

		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				InputStream localFileInputStream = new FileInputStream(value);
				InputStream ResponseFileInputStream = method
						.getResponseBodyAsStream();

				byte[] DataArray = new byte[60 * 1024 * 1024];
				int tmp = 0;

				// reset messagedigest
				messagedigest.reset();

				// read response file
				while (true) {
					tmp = ResponseFileInputStream.read(DataArray, 0,
							DataArray.length);
					if (tmp == -1) {
						break;
					}
					messagedigest.update(DataArray, 0, tmp);
				}
				String responseFileMD5Code = bufferToHex(messagedigest.digest());

				// reset messagedigest
				messagedigest.reset();

				// read local file
				while (true) {
					tmp = localFileInputStream.read(DataArray, 0,
							DataArray.length);
					if (tmp == -1) {
						break;
					}
					messagedigest.update(DataArray, 0, tmp);
				}
				String localFileMD5Code = bufferToHex(messagedigest.digest());

				Assert.assertEquals("local file not equals to response file!",
						localFileMD5Code, responseFileMD5Code);

			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value.isEmpty()) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key).getValue());
				} else {
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/*
	 * map --> key:body value: local file path file size should not large than
	 * byte[60 * 1024 * 1024] offset is for local file
	 */
	public void verifyResponseBodyWithLocalFileInMD5(HttpMethod method,
			Map<String, String> expectMessage, int offset)
			throws HttpException, IOException {

		MessageDigest messagedigest = null;

		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println(VerifyTool.class.getName()
					+ "initialize MD5Util fail!");
			nsaex.printStackTrace();
		}

		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client
				.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(120000);
		int status = client.executeMethod(method);

		Set<String> keySet = expectMessage.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = (String) expectMessage.get(key);
			if (key.equals("body")) {
				InputStream localFileInputStream = new FileInputStream(value);
				InputStream ResponseFileInputStream = method
						.getResponseBodyAsStream();

				byte[] DataArray = new byte[60 * 1024 * 1024];
				int tmp = 0;

				// reset messagedigest
				messagedigest.reset();

				// read response file
				while (true) {
					tmp = ResponseFileInputStream.read(DataArray, 0,
							DataArray.length);
					if (tmp == -1) {
						break;
					}
					messagedigest.update(DataArray, 0, tmp);
				}
				String responseFileMD5Code = bufferToHex(messagedigest.digest());

				// reset messagedigest
				messagedigest.reset();

				// read local file in offset size
				for (int i = 0; i < offset; i++) {
					if (localFileInputStream.read() == -1) {
						break;
					}
				}

				// read local file
				while (true) {
					tmp = localFileInputStream.read(DataArray, 0,
							DataArray.length);
					if (tmp == -1) {
						break;
					}
					messagedigest.update(DataArray, 0, tmp);
				}
				String localFileMD5Code = bufferToHex(messagedigest.digest());

				Assert.assertEquals("local file not equals to response file!",
						localFileMD5Code, responseFileMD5Code);

			} else if (key.equals("status")) {
				Assert.assertEquals(
						"Response status code not equal to expect !!", status,
						Integer.parseInt(value));
			} else {
				if (value.isEmpty()) {
					Assert.assertNull("Response header " + key + "not null!",
							method.getResponseHeader(key).getValue());
				} else {
					Assert.assertEquals("Response header " + key
							+ "not equal to expect !", true, method
							.getResponseHeader(key).getValue().contains(value));
				}
			}
		}

		method.releaseConnection();
		client.getHttpConnectionManager().closeIdleConnections(0);
	}

	/**
	 * 
	 * 字符串工具类.
	 * 
	 * @author diqing 去除字符串首尾出现的某个字符.
	 * @param source
	 *            源字符串.
	 * @param element
	 *            需要去除的字符.
	 * @return String.
	 */
	public String trimFirstAndLastChar(String source, char beginElement,
			char endElement) {
		boolean beginIndexFlag = true;
		boolean endIndexFlag = true;
		do {
			int beginIndex = source.indexOf(beginElement) == 0 ? 1 : 0;
			int endIndex = source.lastIndexOf(endElement) + 1 == source
					.length() ? source.lastIndexOf(endElement) : source
					.length();
			source = source.substring(beginIndex, endIndex);
			beginIndexFlag = (source.indexOf(beginElement) == 0);
			endIndexFlag = (source.lastIndexOf(endElement) + 1 == source
					.length());
		} while (beginIndexFlag || endIndexFlag);
		return source;
	}

	/**
	 * 
	 * json处理工具forlsdir.
	 * 
	 * @author diqing 
	 * @param jResponseStr
	 *            处理的源字符串.
	 * @return String.
	 * @throws JSONException
	 */
	public String setJavaResposeJsonForRestful(String jResponseStr)
			throws JSONException {
		JSONObject j = new JSONObject(jResponseStr);
		String restfulJsonStr = "{\"NAME\":\"" + j.get("fileName")
				+ "\",\"ID\":" + j.get("id") + ",\"SIZE\":"
				+ j.getString("length") + ",\"PID\":" + j.getString("pid")
				+ ",\"VER_NO\":" + j.getLong("version") + "}";
		return restfulJsonStr;
	}
	/**
	 * 
	 * json处理工具for lsfile.
	 * 
	 * @author diqing 
	 * @param jResponseStr
	 *            处理的源字符串.
	 * @return String.
	 * @throws JSONException
	 */
	public String setJavaResposelsFileJsonForRestful(String jResponseStr)
			throws JSONException {
		JSONObject j = new JSONObject(jResponseStr);
		String restfulJsonStr = "{\"ID\":" + j.get("id") + ",\"SIZE\":"
				+ j.getString("length") + ",\"PID\":" + j.getString("pid")
				+ ",\"VER_NO\":" + j.getLong("version") + "}";
		return restfulJsonStr;
	}
}
