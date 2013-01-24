package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Get_AppId extends BaseCase
{
	@Test
	public void test_01_get_right_appkey()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		ExpMeg.Message200.put("APP_ID", "1");
		String App_Key = "tfsNginxA01";
		Ret = GetAppID(App_Key);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		ExpMeg.Message200.remove("APP_ID");
	}
	
	@Test
	public void test_02_get_wrong_appkey()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		
		String App_Key = "AAA";
		Ret = GetAppID(App_Key);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message500);
		
	}
}
