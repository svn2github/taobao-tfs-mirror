package com.taobao.common.tfs.utility;

import java.util.HashMap;
import java.util.Map;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MySQLConnector 
{
	private static Log log = LogFactory.getLog(MySQLConnector.class);
	private static Map<String,Connection> connections = new HashMap<String,Connection>();
	
	public static synchronized Connection getConnection(String server,String db,String user,String pwd)
	{
		user = (user==null?"":user);
		pwd = (pwd==null?"":user);
		
		String key = server+":"+db+":"+user+":"+":"+pwd;
		if(connections.containsKey(key))
		{
			return connections.get(key);
		}
		     else
		{
			Connection connection = createConnection(server,db,user,pwd);
			if(connection!=null)
			{
				connections.put(key, connection);
			}
			return connection;
		}
	}
	
	private static Connection createConnection(String server,String db,String user,String pwd)
	{
		String driverName = "com.mysql.jdbc.Driver";
		Connection connection = null;
		
		String url = "jdbc:mysql://"+server+"/"+db;
		System.out.println(url);
		try 
		{
			Class.forName(driverName).newInstance();
			//connection = DriverManager.getConnection(url,user,pwd);
			connection = DriverManager.getConnection(url+"?user="+user+"&password="+pwd);
		} 
		catch (ClassNotFoundException e)
		{
			log.error("Can not find driver: "+driverName);
			log.error(e.getMessage());
			e.printStackTrace();
			//System.out.println(e.);
		} 
		catch (SQLException e) 
		{
			log.error("SQL exception: "+e.getMessage());
			e.printStackTrace();
		} 
		catch (InstantiationException e) 
		{
			e.printStackTrace();
		} 
		catch (IllegalAccessException e) 
		{
			e.printStackTrace();
		}
		
		return connection;
	}
}
