package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Mv_File extends BaseCase
{
	@Test
	public void test_01_mvFile_right()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_mvFile_exit_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret = CreateFile(App_id,User_id,"test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_mvFile_empty_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_04_mvFile_root_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"/","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_05_mvFile_space_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"       ","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_06_mvFile_empty_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_07_mvFile_root_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","/","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_08_mvFile_space_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","        ","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_mvFile_with_leap_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest/test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_10_mvFile_srcFilePath_same_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test","test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_11_mvFile_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile("2",User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_12_mvFile_not_exit_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_13_mvFile_leap_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest/test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest/test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_14_mvFile_right_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_15_mvFile_exit_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret = CreateFile(App_id,User_id,"test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_16_mvFile_empty_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_17_mvFile_root_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"/","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_18_mvFile_space_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvFile(App_id,User_id,"       ","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_19_mvFile_empty_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_20_mvFile_root_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_21_mvFile_space_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","        ","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_22_mvFile_srcFilePath_same_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test","test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_23_mvFile_circle_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test1/test2/test3/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test1","test1/test2/test3/test4","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test1/test2/test3");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test1/test2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);	
	}
	
	@Test
	public void test_24_mvFile_wrong_appid_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvFile("2",User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_25_mvFile_not_exit_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret.clear();
		Ret = MvFile(App_id,User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
}
