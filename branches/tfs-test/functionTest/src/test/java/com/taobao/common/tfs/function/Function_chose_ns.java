package com.taobao.common.tfs.function;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.RcBaseCase;

public class Function_chose_ns extends RcBaseCase {
	@Test
	public void test_01_happy(){
		boolean bRet = false;
		String localFile = "1k.jpg";
		String sRet = null;
		
		/*init tfsManager*/
		appIp = "";
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
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
	/*unlink file*/
		bRet = tfsManager.unlinkFile(sRet, null);
		Assert.assertTrue(bRet);
		
		sleep(MAX_STAT_TIME);
		
		tfsManager.destroy();
		
		/*init tfsManager*/
		appIp = "";
		tfsManager = new DefaultTfsManager();
		tfsManager.setRcAddr(rcAddr);
		tfsManager.setAppKey(appKey);
		tfsManager.setAppIp(appIp);
		bRet = tfsManager.init();
		Assert.assertTrue(bRet);
		
		oldUsedCapacity = getUsedCapacity(appKey);
		oldFileCount = getFileCount(appKey);
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
}
