package com.taobao.common.tfs.function;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.RcBaseCase;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

/*
 * mazhentong.pt 
 * 
 **/
public class Function_ns_block_duplexing extends RcBaseCase{
	public String localFile = "1k.jpg";
	public String appIp1 = "";
	public String appIp2 = "";
	@Test
	public void test_01_write_happy()
	{
		log.info("test_01_write_happy");
	
		boolean bRet;

		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);
	/*init tfsManager*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp1);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
	/*save file*/
		String sRet = null;
		sRet = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(sRet);

		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		int savecrc = 0;
		int readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);

		tfsManager.destroy();
  
		oldUsedCapacity = getUsedCapacity(appKey);
		oldFileCount = getFileCount(appKey);
	/*init tfsManager*/	
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp2);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
	/*save file*/
		sRet = null;
		sRet=tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(sRet);
			    
		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		savecrc = 0;
		readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);
		
		tfsManager.destroy();
	}
	
	@Test
	public void test_02_write_many_times()
	{
		log.info("test_02_write_many_times");
		
		boolean bRet;

	/*init tfsManager*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp1);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
 
		String sRet = null;
		
		for(int i = 0;i < 15; i++){
			long oldUsedCapacity = getUsedCapacity(appKey);
			long oldFileCount = getFileCount(appKey);
		/*save File*/
			sRet = tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNotNull(sRet);
	
			sleep(MAX_STAT_TIME);
		/*compare capacity and filecount*/
			long newUsedCapacity = getUsedCapacity(appKey);
			long newFileCount = getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
			Assert.assertEquals(oldFileCount + 1, newFileCount);
		/*fetch file*/
			bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
			Assert.assertTrue(bRet);
		/*crc*/
			int savecrc = 0;
			int readcrc = 1;
			savecrc = getCrc(localFile);
			readcrc = getCrc(retLocalFile);
			Assert.assertEquals(savecrc,readcrc);
			
			sleep(MAX_STAT_TIME);
		/*unlink file*/
			bRet = tfsManager.unlinkFile(sRet, null);
			Assert.assertTrue(bRet);
			
			sleep(MAX_STAT_TIME);
			
		}

		tfsManager.destroy();
		
	/*init tfsManager*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp2);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		for(int i = 0;i < 15; i++){
			long oldUsedCapacity = getUsedCapacity(appKey);
			long oldFileCount = getFileCount(appKey);
		/*save file*/	
			sRet = null;
			sRet=tfsManager.saveFile(localFile, null, null, false);
			Assert.assertNotNull(sRet);
				    
			sleep(MAX_STAT_TIME);
		/*compare capacity and filecount*/
			long newUsedCapacity = getUsedCapacity(appKey);
			long newFileCount = getFileCount(appKey);
			Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
			Assert.assertEquals(oldFileCount + 1, newFileCount);
		/*fetch file*/		   
			bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
			Assert.assertTrue(bRet);
		/*crc*/
			int savecrc = 0;
			int readcrc = 1;
			savecrc = getCrc(localFile);
			readcrc = getCrc(retLocalFile);
			Assert.assertEquals(savecrc,readcrc);
			
			sleep(MAX_STAT_TIME);
		/*unlink file*/	
			bRet = tfsManager.unlinkFile(sRet, null);
			Assert.assertTrue(bRet);
		}

		tfsManager.destroy();
	}
	
	@Test
	public void test_03_write_tfsfile_with_other_ns()
	{
		log.info("test_03_write_tfsfile_with_other_ns");
		
		boolean bRet;

		long oldUsedCapacity = getUsedCapacity(appKey);
		long oldFileCount = getFileCount(appKey);
	/*init tfsManager*/
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp1);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
	/*save file*/
		String sRet = null;
		sRet = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(sRet);

		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		long newUsedCapacity = getUsedCapacity(appKey);
		long newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		int savecrc = 0;
		int readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);

		tfsManager.destroy();
  
		oldUsedCapacity = getUsedCapacity(appKey);
		oldFileCount = getFileCount(appKey);
	/*init tfsManager*/	
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp2);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
	/*save file*/
		String sRet1 = null;
		sRet1 = tfsManager.saveFile(localFile, sRet, null, false);
		Assert.assertNotNull(sRet1);
		Assert.assertTrue( sRet1.equals(sRet) );
			    
		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		savecrc = 0;
		readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);
			
		oldUsedCapacity = getUsedCapacity(appKey);
		oldFileCount = getFileCount(appKey);

	/*save file*/
		sRet1 = null;
		sRet1 = tfsManager.saveFile(localFile, null, null, false);
		Assert.assertNotNull(sRet1);

		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet1, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		savecrc = 0;
		readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet1, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);

		tfsManager.destroy();
  
		oldUsedCapacity = getUsedCapacity(appKey);
		oldFileCount = getFileCount(appKey);
	/*init tfsManager*/	
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp1);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
	/*save file*/
		sRet = null;
		sRet = tfsManager.saveFile(localFile, sRet1, null, false);
		Assert.assertNotNull(sRet);
		Assert.assertTrue( sRet.equals(sRet1) );
			    
		sleep(MAX_STAT_TIME);
	/*compare capacity and filecount*/
		newUsedCapacity = getUsedCapacity(appKey);
		newFileCount = getFileCount(appKey);
		Assert.assertEquals(oldUsedCapacity + 1024, newUsedCapacity);
		Assert.assertEquals(oldFileCount + 1, newFileCount);
	/*fetch file*/
		bRet = tfsManager.fetchFile(sRet, null, retLocalFile);
		Assert.assertTrue(bRet);
	/*crc*/
		savecrc = 0;
		readcrc = 1;
		savecrc = getCrc(localFile);
		readcrc = getCrc(retLocalFile);
		Assert.assertEquals(savecrc,readcrc);
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);
		
		tfsManager.destroy();
	}
}
