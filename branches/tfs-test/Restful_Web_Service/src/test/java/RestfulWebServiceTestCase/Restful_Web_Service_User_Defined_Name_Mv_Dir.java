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
		Ret = RmDir(App_id,User_id,"test_dest");
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
	
	@Test
	public void test_05_mvDir_empty_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_06_mvDir_root_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"/","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_07_mvDir_space_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"       ","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_08_mvDir_empty_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_mvDir_root_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","/","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_10_mvDir_space_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","        ","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_11_mvDir_with_leap_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest/test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_12_mvDir_srcFilePath_same_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test","test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_13_mvDir_circle()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test1/test2/test3/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test1","test1/test2/test3/test4","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1/test2/test3");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1/test2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);	
	}
	
	@Test
	public void test_14_mvDir_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir("2",User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_15_mvDir_not_exit_srcFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_16_mvDir_leap_destFilePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest/test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest/test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_17_mvDir_right_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_18_mvDir_srcFilePath_with_File_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test_src/testFile","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test/test/test/test/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_19_mvDir_srcFilePath_with_File_and_Dir_recursive()
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
		Ret = MvDir(App_id,User_id,"test_src","test/test/test/test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test_src/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src/testDir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test/test/test/test/testFile");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test/test/test/test/testDir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_20_mvDir_exit_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret = CreateDir(App_id,User_id,"test_dest","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_21_mvDir_empty_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_22_mvDir_root_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"/","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_23_mvDir_space_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret.clear();
		Ret = MvDir(App_id,User_id,"       ","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_24_mvDir_empty_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_25_mvDir_root_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_26_mvDir_space_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","        ","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_27_mvDir_srcFilePath_same_destFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test","test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_28_mvDir_circle_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test1/test2/test3/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test1","test1/test2/test3/test4","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1/test2/test3");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1/test2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);	
	}
	
	@Test
	public void test_29_mvDir_wrong_appid_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test_src","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = MvDir("2",User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_src");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_30_mvDir_not_exit_srcFilePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret.clear();
		Ret = MvDir(App_id,User_id,"test_src","test_dest","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test_dest");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
}
