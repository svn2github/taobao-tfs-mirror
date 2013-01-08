package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_Original_Delete_Test extends BaseCase
{
	@Test
	public void test_01_writeFile_without_suffix_without_sample_name_delete_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			
		Ret.clear();
		Ret = DeleteFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_02_writeFile_without_suffix_without_sample_name_delete_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_writeFile_with_suffix_without_sample_name_delete_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_04_writeFile_with_suffix_without_sample_name_delete_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_05_writeFile_with_suffix_without_sample_name_delete_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".png",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_06_writeFile_without_suffix_with_sample_name_delete_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_07_writeFile_without_suffix_with_sample_name_delete_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_08_writeFile_with_suffix_with_sample_name_delete_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_writeFile_with_suffix_with_sample_name_delete_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);

	}
	
	@Test
	public void test_10_writeFile_with_suffix_with_sample_name_delete_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".png",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_11_writeFile_without_suffix_without_sample_name_hide_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			
		Ret.clear();
		Ret = DeleteFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_12_writeFile_without_suffix_without_sample_name_hide_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_13_writeFile_with_suffix_without_sample_name_hide_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_14_writeFile_with_suffix_without_sample_name_hide_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_15_writeFile_with_suffix_without_sample_name_hide_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".png","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_16_writeFile_without_suffix_with_sample_name_hide_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_17_writeFile_without_suffix_with_sample_name_hide_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_18_writeFile_with_suffix_with_sample_name_hide_without_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_19_writeFile_with_suffix_with_sample_name_hide_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);

	}
	
	@Test
	public void test_20_writeFile_with_suffix_with_sample_name_hide_wrong_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = DeleteFile(Name,".png","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg",null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,".jpg","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_21_writeFile_delete_wrong_hide_1()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			
		Ret.clear();
		Ret = DeleteFile(Name,null,"-1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_22_writeFile_delete_wrong_hide_2()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		String Name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+Name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			
		Ret.clear();
		Ret = DeleteFile(Name,null,"2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = StatFile(Name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = StatFile(Name,null,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
}
