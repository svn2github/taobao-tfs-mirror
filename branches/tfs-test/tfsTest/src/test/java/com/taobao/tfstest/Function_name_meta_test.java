package com.taobao.tfstest;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import com.taobao.common.tfs.DefaultTfsManager;;

/**
 * @author mingyan
 *
 */
public class Function_name_meta_test extends FailOverBaseCase {
	
	@Test
	public void Function_01_happy_path(){
		DefaultTfsManager tfsManager = new DefaultTfsManager();
		List<String> rootServerAddrList = new ArrayList<String>();
		rootServerAddrList.add("10.232.36.206:10000");
		tfsManager.setRcAddr("10.232.36.203:6100");
		tfsManager.setRootServerAddrList(rootServerAddrList);
		tfsManager.init();
	}
}