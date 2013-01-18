package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Create_File extends BaseCase
{
	@Test
	public void test_01_createFile_right_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}

	@Test
	public void test_02_createFile_leap_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_03_createFile_double_time()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_createFile_empty_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_05_createFile_root_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"/","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_06_createFile_space_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"       ","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"       ");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	

	
	@Test
	public void test_07_createFile_right_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_08_createFile_double_time_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = CreateFile(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = CreateFile(App_id,User_id,"test/test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,5,"test");
	}
	
	@Test
	public void test_09_createFile_empty_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"///////","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_10_createFile_root_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_11_createFile_space_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"       /       /       /       ","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"       /       /       /       ");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	
	
	@Test
	public void test_12_CreateFile_wrong_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_13_CreateFile_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile("2",User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmFile("2",User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_14_CreateFile_wrong_appid_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile("2",User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmFile("2",User_id,"test/test/test/test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
}
