package Tool;

import java.util.HashMap;
import java.util.Map;

public class ExpectMessage 
{
	static public Map<String, String> Message200 = new HashMap<String, String>();
	static public Map<String, String> Message201 = new HashMap<String, String>();
	
	static public Map<String, String> Message400 = new HashMap<String, String>();
	static public Map<String, String> Message401 = new HashMap<String, String>();
	static public Map<String, String> Message403 = new HashMap<String, String>();
	static public Map<String, String> Message404 = new HashMap<String, String>();
	static public Map<String, String> Message409 = new HashMap<String, String>();
	
	static public Map<String, String> Message500 = new HashMap<String, String>();	
	
	static 
	{
		Message200.put("status", "200");
		Message201.put("status", "201");
		
		Message400.put("status", "400");
		Message401.put("status", "401");
		Message403.put("status", "403");
		Message404.put("status", "404");
		Message409.put("status", "409");
		
		Message500.put("status", "500");
	}
}
