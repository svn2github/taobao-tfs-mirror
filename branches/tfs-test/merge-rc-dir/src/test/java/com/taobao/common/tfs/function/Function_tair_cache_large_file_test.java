package com.taobao.common.tfs.function;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.HashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.RcBaseCase;
import com.taobao.common.tfs.impl.FSName;

/**
 * @author Administrator
 */
public class Function_tair_cache_large_file_test extends RcBaseCase{

	public String resourcesPath = "/home/admin/workspace/mazhentong.pt/tfs-test/resource/";
	public String localFile = "1k.jpg";

	public int offset = 0;
	public int length = 1024;
	
	public String key = "/tmp";
	
  	public String tairMasterAddr = "10.232.12.141:5198";
    public String tairSlaveAddr = "10.232.12.141:5198";
    public String tairGroupName = "group_1";
	
    public List<Long> dsList = new ArrayList<Long>();
    private void initTfsManager()
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
	}
    
	/**
	 * 在cache情况下，读写large file
	 * @throws Exception
	 */
	/*use tair cache read ok*/
	@Test
	public void Function_1_taircache_with_large_file_happy_path() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_1_taircache_with_large_file_happy_path";
		log.info(caseName + "===> start");

		tfsManager.init();

		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
		}
	/*use local cache read OK */
public void Function_2_with_localcache_large_file_part_with_localcache() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_2_with_taircache_large_file_part_with_localcache";
		log.info(caseName + "===> start");

		initTfsManager();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(false);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
	}

/*use local cache read OK */
public void Function_3_with_local_taircache_large_file_part_with_localcache() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_2_with_taircache_large_file_part_with_localcache";
		log.info(caseName + "===> start");

	    tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
		
		/*read part file*/
	}
	/*nouse local cache and tair cache read OK */

public void Function_3_with_local_taircache_large_file_with_localcache() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_3_with_local_taircache_large_file_with_localcache";
		log.info(caseName + "===> start");

		tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(false);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
		
		/*read part file*/
		
		
	}
/*part file hit and invalid use local cache cache */

public void Function_4_with_localcache_large_file_part_hitwith_valid() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_4_with_localcache_large_file_part_hitwith_valid";
		log.info(caseName + "===> start");

	    tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(false);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
		
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
		/*read part file*/
	}

/*part file hit and invalid use tair cache cache */

public void Function_5_with_Taircache_large_file_part_hitwith_valid() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_5_with_Taircache_large_file_part_hitwith_valid";
		log.info(caseName + "===> start");

		tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
				String fileName = fsName.get();
				tfsManager.fetchFile(fileName, null, output);
			}
		}
			
			/*clear tair cache*/
			tfsManager.removeRemoteBlockCache(sRet);
		
			/*set local cache invalid*/
		
			tfsManager.insertLocalBlockCache(sRet, dsList);
		
			/*read part file*/
}

/* use tair cache  part_file hit and local cache miss hit */

public void Function_6_with_Taircache_part_file_part_hit_localcahe_miss() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_6_with_Taircache_part_file_part_hit_localcahe_miss";
		log.info(caseName + "===> start");

	    tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
	
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
			}
			}
		
		/*read part file*/

}
/* use tair cache  part_file miss and local cache hit_invalid */

public void Function_7_with_Taircache_part_file_part_hit_localcahe_miss() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp.jpg");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_6_with_Taircache_part_file_part_hit_localcahe_miss";
		log.info(caseName + "===> start");

		tfsManager.init();

		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = sFileName.replaceFirst("T", "L");
		bRet = tfsManager.fetchFile(sFileName, null, output);

		/* get segment info */
		
		String cmd = "/home/admin/workspace/tfs-dev-2.0.1/src/dataserver/view_local_key -f ";
		cmd += sFileName + " | grep block";
		String machine = "10.232.36.206";
		ArrayList<String> result = new ArrayList<String>(500);
		assertTrue(Proc.proStartBase(machine, cmd, result));
		
		if(result != null && result.size() > 0){
			for(String s:result){
				s=s.replaceAll("\\D+", " ");
				s=s.trim();
				String[] strNum = s.split(" ");
				
				FSName fsName = new FSName(Integer.parseInt(strNum[0]), Integer.parseInt(strNum[1]));
			}
			}
		
			/*read part file*/

}
}
		
		
	

