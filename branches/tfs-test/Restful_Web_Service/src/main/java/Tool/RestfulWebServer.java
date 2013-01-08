package Tool;

public class RestfulWebServer 
{
	private String restful_web_server = "0.0.0.0";
	private String tfs_app_key = "";
	
	public String getRestful_web_server()
	{
		return restful_web_server;
	}
	
	public void  setRestful_web_server(String Restul_web_server)
	{
		this.restful_web_server = Restul_web_server;
	}
	
	public String getTfs_app_key()
	{
		return tfs_app_key;
	}
	
	public void  setTfs_app_key(String Tfs_app_key)
	{
		this.tfs_app_key = Tfs_app_key;
	}
}
