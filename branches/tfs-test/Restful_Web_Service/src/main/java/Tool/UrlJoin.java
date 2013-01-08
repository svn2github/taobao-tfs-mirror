package Tool;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class UrlJoin 
{
	protected final static Log log = LogFactory.getLog(UrlJoin.class);
	protected static String BaseUrl = "";
	protected static String Url = "";
	private static boolean init = false; 
	
	public void SetBase(String base)
	{
		BaseUrl = base;
	}
	
	public String GetUrl ()
	{
		if(!init)
		log.info("The Url not init!");
		return Url;
	}
	
	public String GetBase ()
	{
		return BaseUrl;
	}
	
	public void AddDomain(String domain)
	{
		BaseUrl=BaseUrl+"/"+domain;
	}
	
	public void AddUrlDomain(String domain)
	{
		Url=Url+"/"+domain;
	}
	
	public void Init()
	{
		init = true;
		Url = BaseUrl;
	}
	
	public void ResetUrl()
	{
		Url = BaseUrl;
	}
	
	public String AddPara(String Para , String value)
	{
		if(init)
		{
			if(Url.contains("?"))
			   Url=Url+"&"+Para+"="+value;
			else
			   Url=Url+"?"+Para+"="+value;
		}
		else
			log.info("The Url not init!");
		return Url;
	}
	
	public void clean ()
	{
		BaseUrl="";
		Url="";
		init = false;
	}
	
	public void DecDomain(int n)
	{
		String [] each = BaseUrl.split("/");
		BaseUrl = "";
		for(int i=1;i<each.length-n;i++)
			BaseUrl=BaseUrl+"/"+each[i];
	}
}
