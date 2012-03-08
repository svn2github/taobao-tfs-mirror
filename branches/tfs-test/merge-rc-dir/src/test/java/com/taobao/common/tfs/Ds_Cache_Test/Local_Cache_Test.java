package com.taobao.common.tfs.Ds_Cache_Test;

import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.ERcBaseCase;
import com.taobao.common.tfs.RcBaseCase;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.LocalKey;
import com.taobao.common.tfs.impl.SegmentInfo;


public class Local_Cache_Test extends ERcBaseCase
{

    public String localFile = "100k.jpg";
    public String localFileL= "10M.jpg";
    
    public List<Long> dsList = new ArrayList<Long>();
    
    @Before 
    public void setUp()
    {
        dsList.add(12345678L);
        dsList.add(22334455L);
        dsList.add(23456789L);
    }
    @After
    public void tearDown()
    {
        dsList.clear();
    }
    
    public void init ()
    {
    	System.out.println("start tfsManager init");
    	tfsManager = new DefaultTfsManager();
        tfsManager.setRcAddr(rcAddr);
        tfsManager.setAppKey(appKey);
        tfsManager.setAppIp(appIp);
        Assert.assertTrue(tfsManager.init());
    }
    @Test
    public void Function_01_localcache_samll_file() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_01_localcache_samll_file";
        log.info(caseName + "===> start");
        
        System.out.println("start tfsManager init");
    	tfsManager = new DefaultTfsManager();
        tfsManager.setRcAddr(rcAddr);
        tfsManager.setAppKey(appKey);
        tfsManager.setAppIp(appIp);
        Assert.assertTrue(tfsManager.init());
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */
        System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
	public void Function_02_localcache_large_file() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_02_localcache_large_file";
		log.info(caseName + "===> start");

		System.out.println("start tfsManager init");
    	tfsManager = new DefaultTfsManager();
        tfsManager.setRcAddr(rcAddr);
        tfsManager.setAppKey(appKey);
        tfsManager.setAppIp(appIp);
        Assert.assertTrue(tfsManager.init());
        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(getCrc(localFileL), getCrc("temp"));
		
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
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			try
			{
			assertTrue(tfsManager.fetchFile(fileName, null, "10M_part_" + count + ".jpg"));
			}
			catch(Exception e)
			{
				System.out.println("@@@FSname is "+fsName.get());
		    }
		    count++;
		}
		

        tfsManager.destroy();
        log.info(caseName+"===========> end");
	}
    
    @Test
	public void Function_03_localcache_large_file_clean_localcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_03_localcache_large_file_clean_localcache";
		log.info(caseName + "===> start");

		System.out.println("start tfsManager init");
    	tfsManager = new DefaultTfsManager();
        tfsManager.setRcAddr(rcAddr);
        tfsManager.setAppKey(appKey);
        tfsManager.setAppIp(appIp);
        Assert.assertTrue(tfsManager.init());
        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		tfsManager.removeLocalBlockCache(sFileName);
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		assertTrue(bRet);
		Assert.assertEquals(getCrc(localFileL), getCrc("temp"));
		String Tname = sFileName;
		
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
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			try
			{
			   if (count==2)
				   {
				       tfsManager.removeLocalBlockCache(fileName);
				       System.out.println("!!!!!!!");
				   }
			   assertTrue(tfsManager.fetchFile(fileName, null, "10M_part_" + count + ".jpg"));
			}
			catch(Exception e)
			{
				System.out.println("@@@FSname is "+fsName.get());
		    }
		    count++;
		}	
		
		tfsManager.destroy();
        log.info(caseName+"===========> end");
	}
    
    @Test
	public void Function_04_localcache_large_file_inval_Block() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_04_localcache_large_file_clean_Block";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		String Tname = sFileName;
		sFileName = "T" + sFileName.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		
		
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		FSName fsName = new FSName(segmengInfoSet.first().getBlockId(), segmengInfoSet.first().getFileId());
		String fileName = fsName.get();
		
		int count = 0;
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
		    fileName = fsName.get();
		    tfsManager.insertLocalBlockCache(fileName, dsList);
		    count++;
		}
		
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		bRet=tfsManager.fetchFile(Tname, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(getCrc(localFileL), getCrc("temp"));
		
		count = 0;
		System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
		    fileName = fsName.get();
			try
			{
			    assertTrue(tfsManager.fetchFile(fileName, null, "10M_part_" + count + ".jpg"));
			}
			catch(Exception e)
			{
				System.out.println("@@@FSname is "+fsName.get());
		    }
		    count++;
		}
	}
    
    @Test
    public void Function_05_localcache_samll_file_clean_localcache() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_05_localcache_samll_file";
        log.info(caseName + "===> start");
        init();
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
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
        log.info(caseName+"===========> end");
    }
    
    @Test
	public void Function_06_localcache_large_file_inval_localcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_06_localcache_large_file_inval_localcache";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		tfsManager.removeLocalBlockCache(sFileName);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(getCrc(localFileL), getCrc("temp"));
		String Tname = sFileName;
		
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
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			try
			{
			   if (count==2)
				   {
				       for(SegmentInfo segInfonew:segmengInfoSet)
				          {
				    	     FSName fsNamenew = new FSName(segInfonew.getBlockId(), segInfonew.getFileId());
							 String fileNamenew = fsNamenew.get();
				    	     tfsManager.insertLocalBlockCache(fileNamenew, dsList);
				          }
				   }
			   assertTrue(tfsManager.fetchFile(fileName, null, "10M_part_" + count + ".jpg"));
			}
			catch(Exception e)
			{
				System.out.println("@@@FSname is "+fsName.get());
		    }
		    count++;
		}		
	}
    
    @Test
	public void Function_07_localcache_large_file_clean_Block() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_07_localcache_large_file_clean_Block";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		String Tname = sFileName;
		sFileName = "T" + sFileName.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		
		
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		FSName fsName = new FSName(segmengInfoSet.first().getBlockId(), segmengInfoSet.first().getFileId());
		String fileName = fsName.get();
		tfsManager.removeLocalBlockCache(fileName);
		bRet=tfsManager.fetchFile(Tname, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(getCrc(localFileL), getCrc("temp"));
		
		int count = 0;
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
		    fileName = fsName.get();
			try
			{
			    assertTrue(tfsManager.fetchFile(fileName, null, "10M_part_" + count + ".jpg"));
			}
			catch(Exception e)
			{
				System.out.println("@@@FSname is "+fsName.get());
		    }
		    count++;
		}
	}
    
    @Test
    public void Function_08_localcache_samll_file_inval_localcache() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_08_localcache_samll_file_inval_localcache";
        log.info(caseName + "===> start");
        init();
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(false);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
        tfsManager.insertLocalBlockCache(sRet, dsList);
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
        log.info(caseName+"===========> end");
    }
}