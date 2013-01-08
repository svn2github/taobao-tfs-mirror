package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;


import org.junit.Assert;
import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_Original_Save_Test extends BaseCase
{
	@Test
	public void test_01_writeFile_with_right_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,".jpg");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_02_writeFile_with_empty_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,"");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_03_writeFile_with_right_suffix_with_simple_name()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,".jpg");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_04_writeFile_with_empty_suffix_simple_name()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,"");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_05_writeFile_no_suffix_simple_name()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","1",null,null);
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
	}
	
	@Test
	public void test_06_writeFile_with_suffix_wrong_simple_name_1()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","-1",null,".jpg");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_07_writeFile_with_suffix_wrong_simple_name_2()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","2",null,".jpg");
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_08_writeFile_large_without_large_file()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("20M",null,null,null);
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_09_writeFile_small_with_large_file()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,"1",null);
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_10_writeFile_large_with_suffix()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("20M",null,"1",".jpg");
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_11_writeFile_large_with_suffix_simple_name()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("20M","1","1",".jpg");
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_12_writeFile_large_wrong_large_file_1()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("20M",null,"-1",null);
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_13_writeFile_large_wrong_large_file_2()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("20M",null,"2",null);
		String name = Ret.get("TFS_FILE_NAME");
		System.out.println("The return TFS name is "+name);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
		StringBuffer NameBuf = new StringBuffer(name);
		Assert.assertTrue(NameBuf.charAt(0)=='L');	
	}
	
	@Test
	public void test_14_writeFile_no_suffix_wrong_simple_name_1()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","-1",null,null);
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_15_writeFile_no_suffix_wrong_simple_name_2()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K","2",null,null);
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_16_writeFile_empty()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("empty",null,null,null);
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
	
	@Test
	public void test_17_writeFile_empty_large_file()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("empty",null,"1",null);
		System.out.println("The return TFS name is "+Ret.get("TFS_FILE_NAME"));
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message400);
	}
}
