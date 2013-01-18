package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_IsFileExist extends BaseCase
{
	@Test
	public void test_01_isFileExist_right_dirPath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = IsFileExist(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_isFileExist_empty_dirPath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = IsFileExist(App_id,User_id,"");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_03_isFileExist_root_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = IsFileExist(App_id,User_id,"/");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_isFileExist_not_exist_dirPath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = IsFileExist(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_05_isFileExist_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = IsFileExist(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_06_isFileExist_rmDir()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = IsFileExist(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_07_isFileExist_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = IsFileExist("20",User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
}
