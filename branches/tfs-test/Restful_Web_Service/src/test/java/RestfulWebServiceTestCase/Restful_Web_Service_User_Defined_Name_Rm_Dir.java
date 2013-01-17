package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Rm_Dir extends BaseCase
{
	@Test
	public void test_01_rmDir_right_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_rmDir_double_times()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_03_rmDir_not_exist_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();

		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_04_rmDir_empty_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();

		Ret = RmDir(App_id,User_id,"");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_05_rmDir_root_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();

		Ret = RmDir(App_id,User_id,"/");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_06_rmDir_filePath_with_File()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/testFile","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_07_rmDir_filePath_with_Dir()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/testDir","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test/testDir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_08_rmDir_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir("2",User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
	}
}
