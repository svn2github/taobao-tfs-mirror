package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_Original_Fetch_Test extends BaseCase
{
	@Test
	public void test_01_writeFile_read_right()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		assert_tool.AssertCRCEquals("2M", "Temp");
	}
	
	@Test
	public void test_02_writeFile_read_right_with_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"1024",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_writeFile_read_wrong_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"-1",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_04_writeFile_read_more_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,String.valueOf(2*(1<<20)+1),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_05_writeFile_read_right_with_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,"1024");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_06_writeFile_read_wrong_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,"-1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_07_writeFile_read_more_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,String.valueOf(2*(1<<20)+1));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_08_writeFile_read_right_more_size_and_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"1024",String.valueOf(2*(1<<20)-1000));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_writeFile_large_read_right()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("1G",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		long Crc = assert_tool.getCrc("1G");
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		assert_tool.AssertCRCEquals("1G", "Temp");
	}
	
	@Test
	public void test_10_writeFile_large_read_right_with_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,String.valueOf(3*(1<<20)),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_11_writeFile_large_read_wrong_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"-1",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_12_writeFile_large_read_more_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,String.valueOf(30*(1<<20)+1),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_13_writeFile_large_read_right_with_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,String.valueOf(10*(1<<20)));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_14_writeFile_large_read_wrong_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,"-1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_15_writeFile_large_read_more_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,String.valueOf(30*(1<<20)+1));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_16_writeFile_large_read_right_more_size_and_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("30M",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"102400",String.valueOf(30*(1<<20)-10000));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_17_writeFile_large_flag_read_right()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		assert_tool.AssertCRCEquals("10K", "Temp");
	}
	
	@Test
	public void test_18_writeFile_large_flag_read_right_with_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"1024",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_19_writeFile_large_flag_read_wrong_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"-1",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_20_writeFile_large_flag_read_more_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,String.valueOf(10*(1<<10)+1),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_21_writeFile_large_flag_read_right_with_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,String.valueOf(5*(1<<10)));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_22_writeFile_large_flag_read_wrong_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,"-1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_23_writeFile_large_flag_read_more_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,String.valueOf(10*(1<<20)+1));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_24_writeFile_large_flag_read_right_more_size_and_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,"1024",String.valueOf(10*(1<<10)-1000));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_25_writeFile_without_suffix_without_sample_name_read()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".jpg",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_26_writeFile_with_suffix_without_sample_name_read()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".jpg",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".png",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_27_writeFile_without_suffix_with_sample_name_read()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".jpg",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_28_writeFile_with_suffix_with_sample_name_read()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("2M","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".jpg",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(Name,"Temp",".png",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
}
