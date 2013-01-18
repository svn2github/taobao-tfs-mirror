package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Create_Dir extends BaseCase
{
	@Test
	public void test_01_createDir_right_filePath()
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
	public void test_02_createDir_leap_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_03_createDir_double_time()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_createDir_empty_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_05_createDir_root_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"/","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_06_createDir_space_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"       ","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"       ");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_07_createDir_width_100()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int i = 0 ;
		for(i = 0; i<100 ;i++)
		{
			Ret.clear();
			Ret = CreateDir(App_id,User_id,"test"+"_"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		}
		for(i = 0; i<100 ;i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,"test"+"_"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		}
	}
	
	@Test
	public void test_08_createDir_deep_8()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		StringBuilder Dir_Name = new StringBuilder ();
		Dir_Name.append("test");
		int i = 0 ; 
		for (i =0 ;i<8 ;i++)
		{
			Ret = CreateDir(App_id,User_id,Dir_Name.toString(),"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
			Dir_Name.append("/test");
		}
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,8,"test");
	}
	
	@Test
	public void test_09_createDir_width_100_deep_8()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		StringBuilder Dir_Name = new StringBuilder ();
		Dir_Name.append("test");
		int i = 0 ; 
		for (i =0 ;i<8 ;i++)
		{
			Ret = CreateDir(App_id,User_id,Dir_Name.toString(),"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
			if(7!=i)
				Dir_Name.append("/test");
		}
		
		for(i=0;i<100;i++)
		{
			Ret = CreateDir(App_id,User_id,Dir_Name.toString()+"/test"+i,"0");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
		}
		
		for(i=0;i<100;i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,Dir_Name.toString()+"/test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		}
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,8,"test");
	}
	
	@Test
	public void test_10_createDir_right_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,4,"test");
	}
	
	@Test
	public void test_11_createDir_double_time_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = CreateDir(App_id,User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = CreateDir(App_id,User_id,"test/test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,5,"test");
	}
	
	@Test
	public void test_12_createDir_empty_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"///////","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_14_createDir_root_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"/","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_15_createDir_space_filePath_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"       /       /       /       ","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"       /       /       /       ");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_16_createDir_width_100_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int i = 0 ;
		for(i = 0; i<100 ;i++)
		{
			Ret.clear();
			Ret = CreateDir(App_id,User_id,"test"+"_"+i,"1");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		}
		for(i = 0; i<100 ;i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,"test"+"_"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		}
	}
	
	@Test
	public void test_17_createDir_deep_8_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test/test/test/test/test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,8,"test");
	}
	
	@Test
	public void test_18_createDir_width_100_deep_8_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		int i ;
		String Dir_Name = "test/test/test/test/test/test/test/test";
		Ret = CreateDir(App_id,User_id,Dir_Name,"1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		for(i=0;i<100;i++)
		{
			Ret = CreateDir(App_id,User_id,Dir_Name+"/test"+i,"1");
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
		}
		
		for(i=0;i<100;i++)
		{
			Ret.clear();
			Ret = RmDir(App_id,User_id,Dir_Name+"/test"+i);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		}
		StringBuilder Dir_Del_Name = new StringBuilder ();
		Dir_Del_Name.append("test");
		deleteDir(Dir_Del_Name,8,"test");
	}
	
	@Test
	public void test_19_createDir_wrong_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","2");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
	}
	
	@Test
	public void test_20_createDir_wrong_appid()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir("2",User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmDir("2",User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_21_createDir_wrong_appid_recursive()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir("2",User_id,"test/test/test/test","1");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message401);
		
		Ret.clear();
		Ret = RmDir("2",User_id,"test/test/test/test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
}
