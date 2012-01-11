package com.taobao.common.tfs.function;

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import java.util.HashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.RcBaseCase;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.LocalKey;
import com.taobao.common.tfs.impl.SegmentInfo;

/**
 * @author Administrator/lexin
 */
public class Function_tair_cache_test extends RcBaseCase{
	public String appIp1 = "11.232.3.12";
	public String appIp2 = "11.232.5.21";

	public String localFile = "1k.jpg";
	public int offset = 0;
	public int length = 1024;
	
	public String key = "/tmp";
	
  	public String tairMasterAddr = "10.232.12.141:5198";
    public String tairSlaveAddr = "10.232.12.141:5198";
    public String tairGroupName = "group_1";
	
    public List<Long> dsList = new ArrayList<Long>();
    
	/* 加入local/tair cache后读写文件都正常 */
	
	@Test
	public void Function_01_happy_path() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		caseName = "Function_01_happy_path";
		log.info(caseName + "===> start");
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* set cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);
		
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");
	}

	/* test local cache */
	@Test
	public void Function_02_localcache_switch_OK() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);

		caseName = "Function_02_cache_switch_OK";
		log.info(caseName + "===> start");
		
		String sRet = null;
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* set local cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(false);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");
	}

	/* test remote cache */
	@Test
	public void Function_03_remotecache_switch() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_03_remotecache_switch";
		log.info(caseName + "===> start");

		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* set remote cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);
		
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");
	}

	/* test local_cache and remote_cache */
	@Test
	public void Function_04_remote_cache_and_local_cache_switch() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_04_remote_cache_and_local_cache_switch";
		log.info(caseName + "===> start");
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* set cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);
		
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");
	}

	/* test no local_cache and remote_cache */

	@Test
	public void Function_05_nouse_remote_cache_and_local_cache_switch() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_05_nouse_remote_cache_and_local_cache_switch";
		log.info(caseName + "===> start");

		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* close local_cache and remote_cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(false);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");
	}

	/*
	 * 正确插入本地和远程cache本地远程均未命中时,是否能读成功
	 */

	@Test
	public void Function_06_remote_cache_hit_and_local_cache_nonhit_switch() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_06_remote_cache_and_local_cache_nonhit_switch";
		log.info(caseName + "===> start");

		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/* set local_cache and remote_cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/*clear cache*/
		tfsManager.removeLocalBlockCache(sRet);
		tfsManager.removeRemoteBlockCache(sRet);
		
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		log.info(caseName+"==============>");   
	}

	/*local_cache_nonhit remote_cache_hit*/
	
	@Test
	public void Function_07_local_cache_nonhit_and_remote_cache_hit_invalid() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_07_local_cache_nonhit_and_remote_cache_hit_valid";
		log.info(caseName + "===> start");
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*set local and remote cache switch*/
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		/*clear cache*/
		tfsManager.removeLocalBlockCache(sRet);
		
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();
		
		log.info(caseName+"==============>");
	
	}

	/*local_cache_hit_nonhit remote_cache_valid */
	
	@Test
	public void Function_08_local_cache_nonhit_and_remote_cache_hit_valid() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_08_local_cache_nonhit_and_remote_cache_hit";
		log.info(caseName + "===> start");
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		/*clear cache*/
		tfsManager.removeLocalBlockCache(sRet);
		tfsManager.insertRemoteBlockCache(sRet, dsList);
		
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();		
		log.info(caseName+"==============>");
	}
	@Test
	public void Function_09_local_cache_hit_and_remote_cache_nonhit() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_09_local_cache_hit_and_remote_cache_nonhit";
		log.info(caseName + "===> start");
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		/*clear remote cache
		 * set remote cache miss hit
		 * */
		tfsManager.removeRemoteBlockCache(sRet);
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();		
		log.info(caseName+"==============>");
	}
	@Test
	public void Function_10_local_cache_hit_and_remote_cache_invalid() throws Exception {
		
		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_10_local_cache_hit_and_remote_cache_invalid";
		log.info(caseName + "===> start");
		/* tfsManger init*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		/*
		 * set remote cache invalid
		 * */
		tfsManager.insertRemoteBlockCache(sRet, dsList);
		
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();		
		log.info(caseName+"==============>");
	}

	/*local_cache_valid remote_cache_valid*/
	@Test
	public void Function_11_local_cache_valid_and_remote_cache_invalid() throws Exception {

		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String sRet = null;
		
		caseName = "Function_11_local_cache_valid_and_remote_cache_invalid";
		log.info(caseName + "===> start");

		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		
		/* sleep */
		sleep(60);
		
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));

		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		
		/*clear cache*/
		tfsManager.insertLocalBlockCache(sRet, dsList);
		tfsManager.insertRemoteBlockCache(sRet, dsList);
		
		output = null;
		output = new FileOutputStream("tmp");
		/* Read file */		
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		/* Unlink file */
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		tfsManager.destroy();		
		log.info(caseName+"==============>");
	}
	
	/* use tair_cache to read two ns_file and different file*/
	@Test
	public void test_12_tair_cache_with_read_different_file_from_two_ns() throws Exception {

		OutputStream output = new FileOutputStream("tmp");
		byte [] data = getByte(localFile);
		
		boolean bRet = false;
		String[] sRet = new String[2];
		
		caseName = "test_12_tair_cache_with_read_different_file_from_two_ns";
		log.info(caseName + "===> start");
		/*init first ns*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp1);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
			 
		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		sRet[0] = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet[0]);
		
		/* sleep */
		sleep(60);
		
		/*read file*/
		bRet = tfsManager.fetchFile(sRet[0], null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		output = null;
		output = new FileOutputStream("tmp");
		bRet = tfsManager.fetchFile(sRet[0], null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		tfsManager.destroy();
	    
		/*init second ns*/
		
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp2);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		/*start local and remote cache switch*/   
		
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);

		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
		
		/* Write file */
		
		sRet[1] = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet[1]);
		
		Assert.assertFalse( sRet[0].equals(sRet[1]) );
		/* sleep */
		sleep(60);
		
		bRet = tfsManager.fetchFile(sRet[1], null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		/*read file remote cache hit*/	
		bRet = tfsManager.fetchFile(sRet[0], null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
		
		bRet = tfsManager.fetchFile(sRet[1], null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
	}

	/**
	 * 在cache情况下，读写large file
	 * @throws Exception
	 */
	@Test
	public void Function_13_with_large_file_happy_path() throws Exception {
		String segmentFile = "segment_info.jpg";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_13_with_large_file_happy_path";
		log.info(caseName + "===> start");

		initTfsManager();

		/*start local and remote cache switch*/   
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);

		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile("10M.jpg", null, null);
		sFileName = "T" + sFileName.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		/* get segment info */
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		int count = 0;
		for(SegmentInfo segInfo:segmengInfoSet){
			OutputStream filePartOut = new FileOutputStream("tmp_" + count + ".jpg");
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			assertTrue(tfsManager.fetchFile(fileName, null, filePartOut));
			
			filePartOut.flush();
			filePartOut.close();
			
			count++;
		}
	}

	private void initTfsManager()
	{
        ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        appId = tfsManager.getAppId();
	}

}

