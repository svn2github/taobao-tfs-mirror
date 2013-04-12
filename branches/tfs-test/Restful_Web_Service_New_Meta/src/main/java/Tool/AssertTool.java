package Tool;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
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
		
		if(ExpectMeg.containsKey("APP_ID"))
		{
			Assert.assertTrue(Meg.get("APP_ID").equals(ExpectMeg.get("APP_ID")));
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
		System.out.println(LsDirRet);
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
		System.out.println(Ret[0]);
		System.out.println(Ret[1]);
		Assert.assertArrayEquals(Ret, ExpectNum);
	}
	
	char Int2Char(int i )
    {
        switch(i)
        {
            case 1 : return '1';
            case 2 : return '2';
            case 3 : return '3';
            case 4 : return '4';
            case 5 : return '5';
            case 6 : return '6';
            case 7 : return '7';
            case 8 : return '8';
            case 9 : return '9';
            case 10 : return 'a';    
        }
        return 'S';
    }
    
	int Char2Int(char i)
    {
        switch(i)
        {
            case '1' : return 1;
            case '2' : return 2;
            case '3' : return 3;
            case '4' : return 4;
            case '5' : return 5;
            case '6' : return 6;
            case '7' : return 7;
            case '8' : return 8;
            case '9' : return 9;
            case '0' : return 0;    
        }
        return -1;
    }
    
    String CheckString(char [ ] src ,int num)
    {
        String Ret = null;
        int n = src.length;
        String Name = "";
        for(int i =0;i<n;i++)
        {
            if(src[i]=='a')
                break;
            else
                Name+=src[i];
        }
        if(Integer.valueOf(Name)<=num)
            Ret = Name;
        return Ret;
    }
    
	public void GetNumDic(int num ,int numleg, List<String>list)
	{
		list.add("0");
		int FindNum = 1;
		String Tmp = null;
        int i = 0;
        int T1 = 0;
        int offset = 0;
        
        
        char B [ ] =  new char [numleg];
        for(i=0;i<numleg;i++)
        {
            B[i]='a';
        }
        
        B[0]='1';
        list.add(CheckString(B,num));
        
        while(FindNum != num)
        {
            if(offset <(numleg-1))
            {
                if(B[offset+1]=='a')
                {
                    B[++offset]='0';
                    Tmp=CheckString(B,1234);
                    if(Tmp!=null)
                    {
                        //System.out.println(Tmp);
                        list.add(Tmp);
                        ++FindNum;
                        if(FindNum==num)
                        	return;
                    }
                }
            }
            else
            {
                for(T1=1;T1<10;T1++)
                {
                    B[offset]=Int2Char(T1);
                    Tmp=CheckString(B,num);
                    if(Tmp!=null)
                    {
                        //System.out.println(Tmp);
                        list.add(Tmp);
                        ++FindNum;
                        if(FindNum==num)
                        	return;
                    }
                }
                
                B[offset]='a';
                --offset;
                while(B[offset]=='9')
                {
                    B[offset]='a';
                    --offset;
                }
                B[offset]=Int2Char(Char2Int(B[offset])+1);
                
                Tmp=CheckString(B,num);
                if(Tmp!=null)
                {
                    //System.out.println(Tmp);
                    list.add(Tmp);
                    ++FindNum;
                    if(FindNum==num)
                    	return;
                }
                
            }
        }
		
	}
	
}
