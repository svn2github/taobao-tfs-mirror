package com.taobao.common.tfs.Ds_Cache_Test;

import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;


import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.ERcBaseCase;
import com.taobao.common.tfs.RcBaseCase;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.LocalKey;
import com.taobao.common.tfs.impl.SegmentInfo;


public class Remote_Cache_Test extends ERcBaseCase
{

    public String localFile = "100k.jpg";
    public String localFileL= "10M.jpg";
    
    public String tairMasterAddr = "10.232.12.141:5198";
    public String tairSlaveAddr = "10.232.12.141:5198";
    public String tairGroupName = "group_1";
    
    public List<Long> dsList = new ArrayList<Long>();
    
    public void init ()
    {
    	System.out.println("start tfsManager init");
    	tfsManager = new DefaultTfsManager();
        tfsManager.setRcAddr(rcAddr);
        tfsManager.setAppKey(appKey);
        tfsManager.setAppIp(appIp);
        tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr, tairGroupName, 1);
        Assert.assertTrue(tfsManager.init());
    }
    @Test
    public void Function_01_remotecache_samll_file() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_01_remotecache_samll_file";
        log.info(caseName + "===> start");
        init();
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
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
	public void Function_02_remotecache_large_file() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_02_remotecache_large_file";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
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
			
	}
    
    @Test
	public void Function_03_remotecache_large_file_clean_remotecache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_03_remotecache_large_file_clean_remotecache";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(false);
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
	public void Function_04_remotecache_large_file_inval_Block() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_04_remotecache_large_file_inval_Block";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
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
		
		for(SegmentInfo segInfo:segmengInfoSet)
		{
			fsName = new FSName(segInfo.getBlockId(), segInfo.getFileId());
		    fileName = fsName.get();
		    tfsManager.insertRemoteBlockCache(fileName, dsList);
			
		}
		
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
    public void Function_05_remotecache_samll_file_clean_remotecache() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_05_remotecache_samll_file_clean_remotecache";
        log.info(caseName + "===> start");
        init();
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
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
        log.info(caseName+"===========> end");
    }
    
    
    @Test
	public void Function_06_remotecache_large_file_inval_remotecache() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_06_remotecache_large_file_inval_remotecache";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
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
			   if (count==2)
			   {
				   for(SegmentInfo segInfo1:segmengInfoSet)					   
				   {
					   FSName fsName1 = new FSName(segInfo1.getBlockId(), segInfo1.getFileId());
					   String fileName1 = fsName1.get();
					   tfsManager.insertRemoteBlockCache(fileName1, dsList);
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
	public void Function_07_remotecache_large_file_clean_Block() throws Exception 
	{
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;
		caseName = "Function_07_remotecache_large_file_clean_Block";
		log.info(caseName + "===> start");

	    init();
        
	    tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
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
		tfsManager.removeRemoteBlockCache(fileName);
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
    public void Function_08_remotecache_samll_file_inval_remotecache() throws Exception 
    {
        boolean bRet = false;
        OutputStream output = new FileOutputStream("tmp");
        byte [] data = getByte(localFile);

        caseName = "Function_08_remotecache_samll_file_inval_remotecache";
        log.info(caseName + "===> start");
        init();
        
        String sRet;
        /* set local cache */
        tfsManager.setEnableLocalCache(false);
        tfsManager.setEnableRemoteCache(true);
        
        /* Write file */
        sRet = tfsManager.saveFile(data, null, null);
        Assert.assertNotNull(sRet);
              
        /* Read file */     
        bRet = tfsManager.fetchFile(sRet, null, output);
        Assert.assertTrue(bRet);
        
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
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
        log.info(caseName+"===========> end");
    }
    
    //@Test
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
//        tfsManager.setAppIp(appIp1);
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
      //  sleep(60);
        
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
 //       tfsManager.setAppIp(appIp2);
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
    //    sleep(60);
       
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(sRet[1], null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
    /*read file remote cache hit*/ 
        output = null;
        output = new FileOutputStream("tmp"); 
        bRet = tfsManager.fetchFile(sRet[0], null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
        
        output = null;
        output = new FileOutputStream("tmp");
        bRet = tfsManager.fetchFile(sRet[1], null, output);
        Assert.assertTrue(bRet);
        Assert.assertEquals(getCrc(localFile), getCrc("tmp"));
       
        tfsManager.destroy(); 
        log.info(caseName+"===========> end");
    }
}