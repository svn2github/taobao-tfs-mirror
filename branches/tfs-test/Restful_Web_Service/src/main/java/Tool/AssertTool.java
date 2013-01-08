package Tool;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

import org.junit.Assert;

public class AssertTool 
{
	public void AssertMegEquals(Map<String,String> Meg,Map<String,String> ExpectMeg)
	{
		if(ExpectMeg.containsKey("status"))
		{
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
}
