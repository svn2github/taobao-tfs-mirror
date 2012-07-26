package com.taobao.common.tfs.testcase.function.ds;

import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;


import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.LocalKey;
import com.taobao.common.tfs.impl.SegmentInfo;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;


public class Remote_And_Local_Cache_Test extends rcTfsBaseCase
{

    public String localFile = "100k.jpg";
    public String localFileL= "10M.jpg";
    public int offset = 0;
    public int length = 100*(1<<10);
    
    
    public String tairMasterAddr = "10.232.12.141:5198";
    public String tairSlaveAddr = "10.232.12.141:5198";
    public String tairGroupName = "group_1";
    
    public List<Long> dsList = new ArrayList<Long>();
    

    @Test
    public void Function_01_remote_and_local_cache_file() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_01_remote_and_local_cache_file";
        log.info(caseName + "===> start");

        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_02_without_remote_and_local_cache_file() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_02_without_remote_and_local_cache_file";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(false);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_03_remote_and_local_cache_file_close_all() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_03_remote_and_local_cache_file_close_all";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.removeLocalBlockCache(sRet);
        tfsManager.removeRemoteBlockCache(sRet);
        
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.removeRemoteBlockCache(fileName);
	        tfsManager.removeLocalBlockCache(fileName);
		}
		
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_04_remote_and_local_cache_file_close_remote() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_04_remote_and_local_cache_file_close_remote";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.removeRemoteBlockCache(sRet);
        
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.removeRemoteBlockCache(fileName);
		}
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_05_remote_and_local_cache_file_close_local() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_05_remote_and_local_cache_file_close_local";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.removeLocalBlockCache(sRet);
        
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.removeLocalBlockCache(fileName);
		}
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
	public void Function_06_remote_and_local_cache_large_file_clean_localcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_06_remote_and_local_cache_large_file_clean_localcache";
		log.info(caseName + "===> start");


        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		
		sFileName = "T" + sFileName.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		/* get segment info */
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.removeLocalBlockCache(fileName);
		}
		
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));
		
		int count=0;
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			try
			{
			   if (count==2)
				   {
				      for(SegmentInfo segInfo1:segmengInfoSet)
				      {
				    	  FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
						  String fileName1 = fsName1.get();
						  tfsManager.removeLocalBlockCache(fileName1);
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
	public void Function_07_remote_and_local_cache_large_file_clean_remotecache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_07_remote_and_local_cache_large_file_clean_remotecache";
		log.info(caseName + "===> start");


        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));
		
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
				   for(SegmentInfo segInfo1:segmengInfoSet)					   
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.removeRemoteBlockCache(fileName1);
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
	public void Function_08_remote_and_local_cache_large_file_clean_allcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_08_remote_and_local_cache_large_file_clean_allcache";
		log.info(caseName + "===> start");


        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));

		
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
				      for(SegmentInfo segInfo1:segmengInfoSet)
				      {
				    	  FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
						  String fileName1 = fsName1.get();
						  tfsManager.removeRemoteBlockCache(fileName1);
				      }
				   }   
			   if (count==1)
			   {
				      for(SegmentInfo segInfo1:segmengInfoSet)
				      {
				    	  FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
						  String fileName1 = fsName1.get();
						  tfsManager.removeLocalBlockCache(fileName1);
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
    public void Function_09_remote_and_local_cache_file_disable_all() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_09_remote_and_local_cache_file_disable_all";
        log.info(caseName + "===> start");

        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(false);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_10_remote_and_local_cache_file_disable_remote() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_10_remote_and_local_cache_file_disable_remote";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.setEnableRemoteCache(false);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_11_remote_and_local_cache_file_disable_local() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_11_remote_and_local_cache_file_disable_local";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.setEnableLocalCache(false);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_12_remote_and_local_cache_file_inval_all() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_12_remote_and_local_cache_file_inval_all";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.insertRemoteBlockCache(sRet, dsList);
        tfsManager.insertLocalBlockCache(sRet, dsList);
         
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.insertRemoteBlockCache(fileName, dsList);
			tfsManager.insertLocalBlockCache(fileName, dsList);
		}
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_13_remote_and_local_cache_file_inval_remote() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_13_remote_and_local_cache_file_inval_remote";
        log.info(caseName + "===> start");

        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.insertRemoteBlockCache(sRet, dsList);
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.insertRemoteBlockCache(fileName, dsList);
		}
   
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
    public void Function_14_remote_and_local_cache_file_inval_local() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = FileUtility.getByte(localFile);

        caseName = "Function_14_remote_and_local_cache_file_inval_local";
        log.info(caseName + "===> start");

        
        String sRet;
        String lRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
        
        
        lRet = tfsManager.saveLargeFile(localFileL, null, null);
        Assert.assertNotNull(lRet);
        
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet); 
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        
        tfsManager.insertLocalBlockCache(sRet, dsList);
        
        String sFileName = "T" + lRet.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		String segmentFile = "segment_info";
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			FSName fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
			String fileName = fsName.get();
			tfsManager.insertLocalBlockCache(fileName, dsList);
		}
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(sRet, null);
        Assert.assertTrue(bRet);
        
        output = null;
        output = new FileOutputStream("tmp");
        /* Read file */     
        bRet = tfsManager.fetchFile(lRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("tmp"));
        /* Unlink file */
        bRet = tfsManager.unlinkFile(lRet, null);
        Assert.assertTrue(bRet);
        
        tfsManager.destroy();
        log.info(caseName+"===========> end");
    }
    
    @Test
	public void Function_15_remote_and_local_cache_large_file_inval_localcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_15_remote_and_local_cache_large_file_inval_localcache";
		log.info(caseName + "===> start");


        
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));
		
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
				   for(SegmentInfo segInfo1:segmengInfoSet)
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.insertLocalBlockCache(fileName1, dsList);
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
	public void Function_16_remote_and_local_cache_large_file_inval_remotecache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_16_remote_and_local_cache_large_file_inval_remotecache";
		log.info(caseName + "===> start");


	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));
		
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
				   for(SegmentInfo segInfo1:segmengInfoSet)
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.insertRemoteBlockCache(fileName1, dsList);
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
	public void Function_17_remote_and_local_cache_large_file_inval_allcache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_17_remote_and_local_cache_large_file_inval_allcache";
		log.info(caseName + "===> start");

    
	    tfsManager.setEnableLocalCache(true);
        tfsManager.setEnableRemoteCache(true);
        
		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		tfsManager.removeLocalBlockCache(sFileName);
		bRet=tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL), FileUtility.getCrc("temp"));
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
				   for(SegmentInfo segInfo1:segmengInfoSet)
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.insertRemoteBlockCache(fileName1, dsList);
				   }
			   if (count==1)
				   for(SegmentInfo segInfo1:segmengInfoSet)
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.insertLocalBlockCache(fileName1, dsList);
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
}