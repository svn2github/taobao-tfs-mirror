package Tool;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

public class HttpVerifyTool 
{
	public Map<String, String> verifyResponse(HttpMethod method , String File)
	{
		HttpClient client = new HttpClient();
		HttpConnectionManagerParams managerParams = client.getHttpConnectionManager().getParams();
		managerParams.setConnectionTimeout(30000);
		managerParams.setSoTimeout(0);
		int status;
		Map<String, String> Message = new HashMap<String, String>();
		try 
		{
			status = client.executeMethod(method);
			if (null==File)
			{	
				String BodyString = method.getResponseBodyAsString();
				Message.put("status", String.valueOf(status));
				if(null!=BodyString&&!BodyString.isEmpty())
				{
					Message.put("body", BodyString);
					StringBuffer JsonStr = new StringBuffer(BodyString);
					System.out.println(BodyString);
					if(JsonStr.length()!=0)
					{
						if(JsonStr.substring(0,1).equals("{"))
						{
							JSONObject  jsonResponse = new JSONObject(BodyString);
							if(!jsonResponse.isNull("TFS_FILE_NAME"))
								Message.put("TFS_FILE_NAME", jsonResponse.getString("TFS_FILE_NAME"));
							if(!jsonResponse.isNull("SIZE"))
								Message.put("SIZE", jsonResponse.getString("SIZE"));
							if(!jsonResponse.isNull("STATUS"))
								Message.put("STATUS", jsonResponse.getString("STATUS"));
							if(!jsonResponse.isNull("CRC"))
								Message.put("CRC", jsonResponse.getString("CRC"));
							if(!jsonResponse.isNull("APP_ID"))
								Message.put("APP_ID", jsonResponse.getString("APP_ID"));
							if(!jsonResponse.isNull("NAME"))
								Message.put("NAME", jsonResponse.getString("NAME"));
							if(!jsonResponse.isNull("IS_FILE"))
								Message.put("IS_FILE", jsonResponse.getString("IS_FILE"));
						}
					}
				}
				method.releaseConnection();
				client.getHttpConnectionManager().closeIdleConnections(0);
			}
			else
			{
				byte[] b = new byte[10*(1<<20)];
				int readlen;
				FileOutputStream file = new FileOutputStream(File);
				InputStream ResponseFileInputStream = method.getResponseBodyAsStream();
				while(-1!=(readlen=ResponseFileInputStream.read(b)))
				{
					file.write(b,0,readlen);
				}
				file.flush();
				file.close();
				Message.put("status", String.valueOf(status));
			}
		} 
		catch (HttpException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.assertTrue(false);
		} 
		
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.assertTrue(false);
		} 
		catch (JSONException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			Assert.assertTrue(false);
		}
		
		return Message;	
	}

}
