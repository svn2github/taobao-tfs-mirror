package RestfulWebServiceTestCase;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;


public class test extends BaseCase
{
	@Test
	public void aa()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		Ret = SaveFile("10K",null,null,null);
		System.out.println(Ret.get("TFS_FILE_NAME"));
		String name = Ret.get("TFS_FILE_NAME");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret = FetchFile(name,"TEMP",null,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		ExpMeg.Message200.put("SIZE", "10240");
		ExpMeg.Message200.put("STATUS", "0");
		Ret = StatFile(name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		ExpMeg.Message200.remove("SIZE");
		ExpMeg.Message200.remove("STATUS");
		
		Ret.clear();
		Ret = DeleteFile(name,null,null);
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
	}

}
