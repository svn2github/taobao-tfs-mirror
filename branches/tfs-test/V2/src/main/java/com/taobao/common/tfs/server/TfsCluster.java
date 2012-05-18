package com.taobao.common.tfs.server;

import java.util.ArrayList;
import java.util.List;

import com.etao.gaia.server.DefaultCluster;
import com.etao.gaia.server.Server;

public class TfsCluster extends DefaultCluster {
	private boolean masterCluster = false;
	
	public TfsCluster(){
		super("");
	}
	
	public void setMaster(boolean flag){
		masterCluster = flag;
	}
	
	public boolean isMaster(){
		return masterCluster;
	}
	
	public void init(){
		for(Server each:serverList){
			((DefaultTfsServer)each).init();
		}
	}
	
	public List<DefaultTfsServer> getServers(Class <? extends DefaultTfsServer> type){
		List<DefaultTfsServer> results = new ArrayList<DefaultTfsServer>();
		for(Server each:serverList){
			if(each.getClass()==type){
				results.add((DefaultTfsServer)each);
			}
		}
		return results;
	}
}
