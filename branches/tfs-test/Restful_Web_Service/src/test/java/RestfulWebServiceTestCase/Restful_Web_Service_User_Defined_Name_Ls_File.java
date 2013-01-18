package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Ls_File extends BaseCase
{
	@Test
	public void test_01_lsFile_right_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = LsFile(App_id,User_id,"test/test/test/test");
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{1,0});
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_02_lsFile_empty_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret = LsFile(App_id,User_id,"");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);		
	}
	
	@Test
	public void test_03_lsFile_not_exist_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret = LsFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);		
	}
	
	@Test
	public void test_04_lsFile_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = LsFile("20",User_id,"test/test/test/test");
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{1,0});
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_05_lsFile_complex()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int AllNum = 0;
		int FileNum = 0 ;
		int i ,j ;
		for(i=0; i<5; i++)
		{
			Ret.clear();
			Ret = CreateDir(App_id,User_id,"test"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
			Ret = CreateFile(App_id,User_id,"test"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			AllNum=+2;
			++FileNum;
		}
		
		for(i=0; i<5; i++)
		{
			for(j=0; j<5; j++)
			{
				Ret.clear();
				Ret = CreateDir(App_id,User_id,"test"+i+"/test"+j,"0");
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
				Ret.clear();
				Ret = CreateFile(App_id,User_id,"test"+i+"/test"+j,"0");
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
				AllNum=+2;
				++FileNum;
			}
		}
		
		Ret.clear();
		Ret = LsFile(App_id,User_id,"/");
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{AllNum,FileNum});
		
		for(i=0; i<5; i++)
		{
			for(j=0; j<5; j++)
			{
				Ret.clear();
				Ret = RmDir(App_id,User_id,"test"+i+"/test"+j);
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
				Ret.clear();
				Ret = RmFile(App_id,User_id,"test"+i+"/test"+j);
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
				AllNum=+2;
				++FileNum;
			}
		}
		
		for(i=0; i<5; i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,"test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
			Ret = RmFile(App_id,User_id,"test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			AllNum=+2;
			++FileNum;
		}
		
	}
	
	@Test
	public void test_06_lsFile_root()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret = LsFile(App_id,User_id,"/");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);		
	}
}
