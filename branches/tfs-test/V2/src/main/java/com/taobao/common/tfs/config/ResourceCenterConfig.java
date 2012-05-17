package com.taobao.common.tfs.config;

import java.io.IOException;

public class ResourceCenterConfig extends Config {
	private int mysqlPort;
	private String mysqlAddr;
	private String user;
	private String password;
	private String tfsStatusDB;
	
	public void setMysqlPort(int port){
		this.mysqlPort = port;
	}
	
	public void setMysqlAddr(String addr){
		this.mysqlAddr = addr;
	}
	
	public void setUser(String user){
		this.user = user;
	}
	
	public void setPassword(String password){
		this.password = password;
	}
	
	public void setTfsStatusDB(String tfsStatusDB){
		this.tfsStatusDB = tfsStatusDB;
	}
	
	public int getMysqlPort(){
		return this.mysqlPort;
	}
	
	public String getMysqlAddr(){
		return this.mysqlAddr;
	}
	
	public String getUser(){
		return this.user;
	}
	
	public String getPassword(){
		return this.password;
	}
	
	public String getTfsStatusDB(){
		return this.tfsStatusDB;
	}
	
	@Override
	public void init() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
