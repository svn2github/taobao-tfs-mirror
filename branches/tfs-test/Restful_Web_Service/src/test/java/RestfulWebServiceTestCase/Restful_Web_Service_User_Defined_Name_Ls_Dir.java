package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Ls_Dir extends BaseCase 
{
	@Test
	public void test_01_lsDir_right_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = LsDir(App_id,User_id,"test/test/test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{1,0});
		
		String Dir_Del_Name = "test";
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_02_lsDir_empty_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret = LsDir(App_id,User_id,"");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);		
	}
	
	@Test
	public void test_03_lsDir_not_exist_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
	
		Ret = LsDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);		
	}
	
	@Test
	public void test_04_lsDir_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = LsDirNeedAppKey("tfsNginxC01",App_id,User_id,"test/test/test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{1,0});
		
		String Dir_Del_Name = "test";
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_05_lsDir_complex()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int AllNum = 10;
		int FileNum = 5 ;
		int i ,j ;
		for(i=0; i<5; i++)
		{
			Ret.clear();
			Ret = CreateDir(App_id,User_id,"test"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
			Ret = CreateFile(App_id,User_id,"test"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
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
			}
		}
		
		System.out.println("!"+AllNum);
		System.out.println("!"+FileNum);
		Ret.clear();
		Ret = LsDir(App_id,User_id,"/");
		assert_tool.AssertLsDirEquals(Ret.get("body"),new int[]{AllNum,FileNum});
		
		for(i=0; i<5; i++)
		{
			for(j=0; j<5; j++)
			{
				Ret.clear();
				Ret = RmDir(App_id,User_id,"test"+i+"/test"+j);
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
				Ret.clear();
				Ret = RmFile(App_id,User_id,"test"+i+"/test"+j);
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
				AllNum=+2;
				++FileNum;
			}
		}
		
		for(i=0; i<5; i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,"test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			Ret.clear();
			Ret = RmFile(App_id,User_id,"test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			AllNum=+2;
			++FileNum;
		}
		
	}
}

