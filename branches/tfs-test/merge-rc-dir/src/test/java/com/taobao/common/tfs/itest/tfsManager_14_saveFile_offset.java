package com.taobao.common.tfs.itest;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.Ignore;

import com.taobao.common.tfs.tfsNameBaseCase;

public class tfsManager_14_saveFile_offset extends tfsNameBaseCase 
{
	@Test
    public  void  test_01_saveFile_byte() throws IOException
	{
		log.info( "test_01_saveFile_byte" );

        String tfsFile = "/test01SFB";
		boolean bRet = false;

		byte data[] = null;    
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), tfsFile);
		Assert.assertTrue(bRet);
        
        bRet = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertTrue(bRet);
	}

    @Test
    public  void  test_02_saveFile_byte_with_empty_data_1() throws IOException
    {
        log.info( "test_02_saveFile_byte_with_empty_data_1" );
        boolean bRet = false;

        byte data[] = null;
        String buf = "";
        data = buf.getBytes();

        bRet = tfsManager.saveFile(appId, userId, data, 0, 0, "/test02SFBWED1");
        Assert.assertTrue(bRet);
    }

    @Test
    public  void  test_03_saveFile_byte_with_empty_data_2() throws IOException
    {
        log.info( "test_03_saveFile_byte_with_empty_data_2" );
        boolean bRet = false;
    
        byte data[] = null;
        String buf = "";
        data = buf.getBytes();

        bRet = tfsManager.saveFile(appId, userId, data, 0, 100, "/test03SFBWED2");
        Assert.assertFalse(bRet);
    }

    @Test
    public  void  test_04_saveFile_byte_with_null_data_1() throws IOException
    {
        log.info( "test_04_saveFile_byte_with_null_data_1" );

        boolean bRet = false;

        byte data[] = null;

        bRet = tfsManager.saveFile(appId, userId, data, 0, 0, "/test04SFBWND1");
        Assert.assertFalse(bRet);
    }

    @Test
    public  void  test_05_saveFile_byte_with_null_data_2() throws IOException
    {
        log.info( "test_05_saveFile_byte_with_null_data_2" );

        boolean bRet = false;

        byte data[] = null;

        bRet = tfsManager.saveFile(appId, userId, data, 0, 100, "/test05SFBWND2");
        Assert.assertFalse(bRet);
    }

    @Test
    public  void  test_06_saveFile_byte_wrong_offset() throws IOException
    {
        log.info( "test_06_saveFile_byte_wrong_offset" );
        boolean bRet = false;

        byte data[] = null;
        data = getByte(resourcesPath+"100k.jpg");

        bRet = tfsManager.saveFile(appId, userId, data, -1, 100*(1<<10), "/test06SFBWO");
        Assert.assertFalse(bRet);
    }

    @Test
    public  void  test_07_saveFile_byte_with_offset_less_length_and_more_zero() throws IOException
    {
        log.info( "test_07_saveFile_byte_with_offset_less_length_and_more_zero" );

        String tfsFile = "/test07SFBWOLLAMZ";
        boolean bRet = false;

        byte data[] = null;
        data = getByte(resourcesPath+"100k.jpg");

        bRet = tfsManager.saveFile(appId, userId, data, 10*(1<<10), 90*(1<<10), tfsFile);
        Assert.assertTrue(bRet);

        bRet = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertTrue(bRet);
    }

    @Test
    public  void  test_08_saveFile_byte_with_offset_more_length() throws IOException
    {
        log.info( "test_08_saveFile_byte_with_offset_more_length" );

        boolean bRet = false;

        byte data[] = null;
        data = getByte(resourcesPath+"100k.jpg");

        bRet = tfsManager.saveFile(appId, userId, data, 110*(1<<10), 100*(1<<10), "/test08SFBWOML");
        Assert.assertFalse(bRet);
    }

	@Test
    public  void  test_09_saveFile_byte_less_length() throws IOException
	{
		log.info( "test_09_saveFile_byte_less_length" );

        String tfsFile = "/test09SFBLL";
		boolean bRet= false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 10*(1<<10), tfsFile);
		Assert.assertTrue(bRet);

		bRet = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertTrue(bRet);
	}
	
	@Test
    public  void  test_10_saveFile_byte_more_length() throws IOException
	{
		log.info( "test_10_saveFile_byte_more_length" );
		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 200*(1<<10), "/test10SFBML");
		Assert.assertFalse(bRet);
	}
	
	@Test
    public  void  test_11_saveFile_byte_with_offset_and_length() throws IOException
	{
		log.info( "test_11_saveFile_byte_with_offset_and_length" );

        String tfsFile = "/test11SFBWOAL";
		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 10*(1<<10), 60*(1<<10), tfsFile);
		Assert.assertTrue(bRet);

		bRet = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertTrue(bRet);
	}
	
	
	@Test
    public  void  test_12_saveFile_byte_with_wrong_length() throws IOException
	{
		log.info( "test_12_saveFile_byte_with_wrong_length" );

		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, -1, "/test12SFBWWL");
		Assert.assertTrue(bRet);
	}
	
	@Test
    public  void  test_13_saveFile_byte_with_file_exist() throws IOException
	{
		log.info( "test_13_saveFile_byte_with_file_exist" );

        String tfsFile = "/test13SFBWFE";
		boolean bRet = false;
		
		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), tfsFile);
		Assert.assertTrue(bRet);

        bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), tfsFile);
        Assert.assertFalse(bRet);

        bRet = tfsManager.rmFile(appId, userId, tfsFile);
        Assert.assertTrue(bRet);
	}
	
	@Test
    public  void  test_14_saveFile_byte_with_wrong_fileName_1() throws IOException
	{
		log.info( "test_14_saveFile_byte_with_wrong_fileName_1" );

		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), "test14SFBWWTN");
		Assert.assertFalse(bRet);
	}
	
	@Test
    public  void  test_15_saveFile_byte_with_wrong_fileName_2() throws IOException
	{
		log.info( "test_15_saveFile_byte_with_wrong_fileName_2" );

		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100m.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<20), "/");
		Assert.assertFalse(bRet);
	}
	
	@Test
    public  void  test_16_saveFile_byte_with_empty_fileName() throws IOException
	{
		log.info( "test_16_saveFile_byte_with_empty_fileName" );

		boolean bRet = false;

		byte data[] = null;
		data = getByte(resourcesPath+"100k.jpg");

		bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), "");
		Assert.assertFalse(bRet);
	}
    
    @Test
    public  void  test_17_saveFile_byte_with_null_fileName() throws IOException
    {
        log.info( "test_17_saveFile_byte_with_empty_fileName" );

        boolean bRet = false;

        byte data[] = null;
        data = getByte(resourcesPath+"100k.jpg");
        
        bRet = tfsManager.saveFile(appId, userId, data, 0, 100*(1<<10), null);
        Assert.assertFalse(bRet);
    }
}
