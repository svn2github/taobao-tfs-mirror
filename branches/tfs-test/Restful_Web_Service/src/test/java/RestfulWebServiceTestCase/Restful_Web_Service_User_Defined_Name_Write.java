package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Write extends BaseCase
{
	@Test
	public void test_01_WriteFile_right_filePath()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_write_more_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = 2*(1<<20)+1;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_write_n_1_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = -1;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_write_wrong_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = -2;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}

	@Test
	public void test_05_write_with_offset()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = 1<<20;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_06_write_more_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer size = 2*(1<<20)+1024;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_07_write_wrong_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer size = -1;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_08_write_with_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer size = 10;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_09_write_with_offset_and_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = 2*(1<<20)+1;
		Integer size = 1<<20;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_10_write_more_offset_and_size()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset = 2*(1<<20)+1;
		Integer size = 2*(1<<20)+1024;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_11_write_empty_data()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","empty",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_12_write_Dir()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateDir(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmDir(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_13_write_many_times()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_14_write_not_exist()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = WriteFile(App_id,User_id,"test","2M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message404);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_15_write_large()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","1G",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_16_write_many_times_parts()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset;
		Integer size;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 10*(1<<10);
		offset=10*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 10*(1<<10);
		offset=20*(1<<10)+1;
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		offset=20*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","1B",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		offset=20*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","1B",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_17_write_large_many_times_parts()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset;
		Integer size;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","3M",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 3*(1<<20);
		offset=3*(1<<30);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","3M",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 3*(1<<20);
		offset=6*(1<<20)+1;
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		offset=6*(1<<20);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","1B",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		offset=6*(1<<20);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","1B",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_18_write_many_times_parts_com()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Integer offset;
		Integer size;
		
		Ret = CreateFile(App_id,User_id,"test","0");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		
		
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 2*(1<<20);
		offset=20*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","2M",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 3*(1<<20);
		offset=8*(1<<20)+20*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","3M",offset.toString(),size.toString());
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 10*(1<<10);
		offset = 10*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		size = 10*(1<<10);
		offset = 0;
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","10K",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message409);
		
		size = 3*(1<<20);
		offset=2*(1<<20)+20*(1<<10);
		Ret.clear();
		Ret = WriteFile(App_id,User_id,"test","3M",offset.toString(),null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = RmFile(App_id,User_id,"test");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
}
