package Tool;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;

public class AssertTool 
{
	public void AssertMegEquals(Map<String,String> Meg,Map<String,String> ExpectMeg)
	{
		if(ExpectMeg.containsKey("status"))
		{
			System.out.println("status is : "+Meg.get("status"));
			Assert.assertTrue(Meg.get("status").equals(ExpectMeg.get("status")));
		}
		if(ExpectMeg.containsKey("SIZE"))
		{
			Assert.assertTrue(Meg.get("SIZE").equals(ExpectMeg.get("SIZE")));
		}
		if(ExpectMeg.containsKey("STATUS"))
		{
			Assert.assertTrue(Meg.get("STATUS").equals(ExpectMeg.get("STATUS")));
		}
	}
	public void AssertCRCEquals(String localfile,String file)
	{
		Assert.assertEquals(getCrc(localfile), getCrc(file));
	}
	public long getCrc(String fileName) 
	{
		FileInputStream input = null;
		CRC32 crc = new CRC32();
		try 
		{
			input = new FileInputStream(fileName);
			int readLength;
			byte[] data = new byte[102400];
			
			crc.reset();
			while ((readLength = input.read(data, 0, 102400)) > 0) 
			{
				crc.update(data, 0, readLength);
			}
			input.close();
		} 
		catch (IOException e) 
		{
			e.getStackTrace();
		}
		return crc.getValue();
	}
	
	public int[] dealWithLsDir(String LsDirRet)
	{
		StringBuffer Str = new StringBuffer(LsDirRet);
	
		int allNum=0;
		int fileNum=0;
		int start;
		int end;
		
		while( -1!=(start=Str.indexOf("{")))
		{
			++allNum;
			end = Str.indexOf("}");
			try 
			{
				JSONObject  jsonResponse = new JSONObject(Str.substring(start, end+1));
				if(jsonResponse.getString("IS_FILE").contains("true"))
				++fileNum;
			} 
			catch (JSONException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Str.delete(0, end+1);
		}		
		int [] Ret = new int [2];
		Ret[0]= allNum;
		Ret[1]= fileNum;
		return Ret;
	}
	
	public void AssertLsDirEquals(String LsDirRet,int [] ExpectNum)
	{
		int [] Ret = new int [2];
		Ret = dealWithLsDir(LsDirRet);
		Assert.assertArrayEquals(Ret, ExpectNum);
	}
}
