package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Mv_Dir extends BaseCase
{
	@Test
	public void test_01_mvDir_right()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_mvDir_srcFilePath_with_File()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src/testFile","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_mvDir_srcFilePath_with_File_and_Dir()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src/testFile","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = CreateDir(App_id,User_id,"test_src/testDir","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src/testDir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest/testDir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_mvDir_exit_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret = CreateDir(App_id,User_id,"test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
}
