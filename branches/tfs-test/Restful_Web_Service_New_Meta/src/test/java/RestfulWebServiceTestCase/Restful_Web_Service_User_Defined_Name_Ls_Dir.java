package RestfulWebServiceTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import Tool.AssertTool;
import Tool.ExpectMessage;

public class Restful_Web_Service_User_Defined_Name_Ls_Dir extends BaseCase
{
	@Test
	public void test_01_lsDir_allFile()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int i;
		int n = 999;

		for (i = 0; i <= n; i++)
		{
			Ret = CreateFile(App_id, User_id, "test" + String.valueOf(i));
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
			Ret.clear();
		}

		Ret = LsDir(App_id, User_id, "/");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);

		List list = new ArrayList();
		assert_tool.GetNumDic(n, 3, list);

		for (i = 0; i < list.size(); ++i)
			System.out.println(list.get(i));

		try
		{
			JSONArray ObjectArray = new JSONArray(Ret.get("body"));
			for (i = 0; i < n; i++)
			{
				JSONObject object = ObjectArray.getJSONObject(i);
				Assert.assertEquals(object.get("NAME"), "test" + list.get(i));
				Assert.assertEquals(object.get("IS_FILE"), true);
			}
		}
		catch (JSONException e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}

		Ret.clear();
		for (i = 0; i <=n; i++)
		{
			Ret = RmFile(App_id, User_id, "test" + String.valueOf(i));
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			Ret.clear();
		}
	}

	// 测试下面Case时候，KV Meta Server 那边 MAX_LIMIT需要进行修改改为3
	@Test
	public void test_02_lsDir_File_And_Dir()
	{
		Map<String, String> Ret = new HashMap<String, String>();
		ExpectMessage ExpMeg = new ExpectMessage();
		AssertTool assert_tool = new AssertTool();
		int i;
		int DirLv = 4;

		Ret = CreateFile(App_id, User_id, "/Dir/Dir/Dir/Dir/File");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message201);
		Ret.clear();

		String Name = "/";
		try
		{
			for (i = 0; i < DirLv; ++i)
			{
				Ret = LsDir(App_id, User_id, Name);
				assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);

				JSONArray ObjectArray = new JSONArray(Ret.get("body"));
				
				JSONObject object =ObjectArray.getJSONObject(0);
				Assert.assertEquals(object.get("NAME"), "Dir");
				Assert.assertEquals(object.get("IS_FILE"), false);
				Ret.clear();
				Name = Name+"Dir/";
			}
			
			Ret = LsDir(App_id, User_id, Name);
			assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
			
			JSONArray ObjectArray = new JSONArray(Ret.get("body"));
			JSONObject object =ObjectArray.getJSONObject(0);
			Assert.assertEquals(object.get("NAME"), "File");
			Assert.assertEquals(object.get("IS_FILE"), true);
		}
		catch (JSONException e)
		{
			e.printStackTrace();
			Assert.assertTrue(false);
		}

		Ret.clear();
		Ret=RmDir(App_id, User_id,"/Dir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message403);
		
		Ret.clear();
		Ret=RmFile(App_id, User_id,"/Dir/Dir/Dir/Dir/File");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret=RmDir(App_id, User_id,"/Dir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret=RmDir(App_id, User_id,"/Dir/Dir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret=RmDir(App_id, User_id,"/Dir/Dir/Dir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
		Ret.clear();
		Ret=RmDir(App_id, User_id,"/Dir/Dir/Dir/Dir");
		assert_tool.AssertMegEquals(Ret, ExpMeg.Message200);
		
	}
}
