package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_Original_Stat_Test extends BaseCase
{
	@Test
	public void test_01_writeFile_without_suffix_without_sample_name_stat_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		ExpMeg.Message200.put("CRC", Long.toString(assert_tool.getCrc("10K")));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		ExpMeg.Message200.remove("CRC");
	}
	
	@Test
	public void test_02_writeFile_without_suffix_without_sample_name_stat_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_03_writeFile_with_suffix_without_sample_name_stat_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		ExpMeg.Message200.put("CRC", Long.toString(assert_tool.getCrc("10K")));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		ExpMeg.Message200.remove("CRC");
	}
	
	@Test
	public void test_04_writeFile_with_suffix_without_sample_name_stat_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		ExpMeg.Message200.put("CRC", Long.toString(assert_tool.getCrc("10K")));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		ExpMeg.Message200.remove("CRC");
	}
	
	@Test
	public void test_05_writeFile_with_suffix_without_sample_name_stat_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".png",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_06_writeFile_without_suffix_with_sample_name_stat_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		ExpMeg.Message200.put("CRC", Long.toString(assert_tool.getCrc("10K")));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		ExpMeg.Message200.remove("CRC");
	}
	
	@Test
	public void test_07_writeFile_without_suffix_with_sample_name_stat_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_08_writeFile_with_suffix_with_sample_name_stat_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_writeFile_with_suffix_with_sample_name_stat_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		ExpMeg.Message200.put("CRC", Long.toString(assert_tool.getCrc("10K")));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		ExpMeg.Message200.remove("CRC");
	}
	
	@Test
	public void test_10_writeFile_with_suffix_with_sample_name_stat_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".png",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_11_writeFile_stat_wrong_type_1()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"-1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_12_writeFile_stat_wrong_type_2()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
}
