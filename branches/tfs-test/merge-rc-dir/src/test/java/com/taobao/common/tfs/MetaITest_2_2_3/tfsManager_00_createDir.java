package com.taobao.common.tfs.MetaITest_2_2_3;


import org.junit.Test;
import org.junit.Ignore;
import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;



public class tfsManager_00_createDir extends tfsNameBaseCase 
{

    @Test
    public  void  test_01_createDir_right_filePath()
    {
    	int Ret;
	    log.info( "test_01_createDir_right_filePath" );
        Ret=tfsManager.createDir(appId, userId, "/textcreateDir1");
        Assert.assertEquals("Create Dir with right path should be true", Ret,0);
        tfsManager.rmDir(appId, userId, "/textcreateDir1");
     }
	
	@Test 
	public void  test_02_createDir_leap_filePath()
	{
	   int Ret;
	   log.info( "test_02_createDir_leap_filePath" );
	   Ret=tfsManager.createDir(appId, userId, "/textcreateDir2/test");
	   Assert.assertEquals("Create Dir with leap path should be false", Ret,-14002);
	}
	@Test
	public void  test_03_createDir_double_time()
	{
	   int Ret;
	   log.info( "test_03_createDir_double_time" );

	   Ret=tfsManager.createDir(appId, userId, "/textcreateDir3dt"); 
	   Assert.assertEquals("Create Dir with right path should be true", Ret,0);
	   Ret=tfsManager.createDir(appId, userId, "/textcreateDir3dt");
	   Assert.assertEquals("Create Dir with two times should be false", Ret,-14001);

	   Ret = tfsManager.rmDir(appId, userId, "/textcreateDir3dt");
       Assert.assertEquals(Ret,0);
	}
	@Test
	public void  test_04_createDir_null_filePath()
	{
	   int Ret;
	   log.info( "test_04_createDir_null_filePath" );
	   Ret=tfsManager.createDir(appId, userId, null);
	   Assert.assertEquals("Create Dir with null path should be false", Ret,-14010);
	}
	@Test
	public void  test_05_createDir_empty_filePath()
	{
	   int Ret;
	   log.info( "test_04_createDir_null_filePath" );
	   Ret=tfsManager.createDir(appId, userId, "");
	   Assert.assertEquals("Create Dir with null path should be false", Ret,-14010);
	}
	@Test
	public void  test_06_createDir_with_same_fileName()
	{
	   int Ret;
	   log.info( "test_06_createDir_with_same_fileName" );
	   tfsManager.createFile(appId, userId, "/textcreateDir6wsfn");
	   Ret=tfsManager.createDir(appId, userId, "/textcreateDir6wsfn");
	   Assert.assertEquals("Create Dir with the same File name should be true", Ret,0);
	   tfsManager.rmFile(appId, userId, "/textcreateDir6wsfn");
	   tfsManager.rmDir(appId, userId, "/textcreateDir6wsfn");
	}
	@Test
	public void  test_07_createDir_wrong_filePath_1()
	{
	   int Ret;
	   log.info( "test_07_createDir_wrong_filePath_1" );
	   Ret=tfsManager.createDir(appId, userId, "textcreateDir7wfp");
	   Assert.assertEquals("Create Dir with wrong_1 path should be false", Ret,-14010);
	}
	@Test
	public  void  test_08_createDir_wrong_filePath_2()
	{
	   int Ret;
	   log.info( "test_08_createDir_wrong_filePath_2" );
	   Ret=tfsManager.createDir(appId, userId, "////textcreateDir8wfp////");
	   Assert.assertEquals("Create Dir with wrong_2 path be true", Ret,0);
	   tfsManager.rmDir(appId, userId, "/textcreateDir8wfp");
	}
	@Test
	public void  test_09_createDir_wrong_filePath_3()
	{
	   int Ret;
	   log.info( "test_09_createDir_wrong_filePath_3" );
	   Ret=tfsManager.createDir(appId, userId, "/");
	   Assert.assertEquals("Create Dir with wrong_3 path should be false", Ret,-14010);
	}
	@Test
	public void  test_10_createDir_width_100()
	{
	   int Ret;
	   log.info( "test_10_createDir_width_100" );
	   int i;
	   for(i=1;i<=98;i++)
	   {
	       Ret=tfsManager.createDir(appId, userId, "/text"+i);
	       Assert.assertEquals("Create text"+i+" should be true", Ret,0);
	   }
	   
	   for(i=1;i<=98;i++)
	   {
	     tfsManager.rmDir(appId, userId, "/text"+i);
	   }
	}
	@Test
	public void  test_11_createDir_deep_8()
	{
	   int Ret;
	   log.info( "test_11_createDir_deep_8" );
	   int i;
	   String s="/text";
	   for(i=0;i<8;i++)
	   {
	       Ret=tfsManager.createDir(appId, userId, s);
	       Assert.assertEquals("Create text should be true", Ret,0);
	       s="/text"+s;
	   }
	   tfsNameBaseCase BaseCase =new tfsNameBaseCase();
	   BaseCase.deleteDir("/text", 8, tfsManager);
	   tfsManager.rmDir(appId, userId, "/text");
    }
	@Test
	public void  test_12_createDir_width_100_deep_7()
	{
	   int Ret;
	   log.info( "test_12_createDir_width_100_deep_7" );
	   int i;
	   String s="/text";
	   for(i=0;i<7;i++)
	   {
	       Ret=tfsManager.createDir(appId, userId, s);
	       Assert.assertEquals("Create text should be true", Ret,0);
	       if(i!=6)s="/text"+s;
	   }
	   
	   for(i=1;i<=100;i++)
	   {
		   Ret=tfsManager.createDir(appId, userId, s+"/text"+i);
		   if (Ret!=0)
		   {
		      System.out.println(s+"/text"+i);
		   }
		   Assert.assertEquals("Create text should be true", Ret,0);
	   }
	   
	   for(i=1;i<=100;i++)
	   {
		   tfsManager.rmDir(appId, userId, s+"/text"+i);
	   }
	   tfsNameBaseCase BaseCase =new tfsNameBaseCase();
	   BaseCase.deleteDir("/text", 7, tfsManager);
	   tfsManager.rmDir(appId, userId, "/text");
    }
	@Test
	public void test_13_createDir_length_more_512()
    {
 	    int Ret;
	    log.info( "test_13_createDir_length_more_512" );
	    int len = 512;
        String s = new String();  
	    for(int i=0;i<len;i++)
        {
            int a = '0' + i%40;
               s += (char)a;               
        }      
	    s ="/"+s;
	    Ret=tfsManager.createDir(appId, userId, s);
		Assert.assertEquals("Create Dir with more length should be false", Ret,-14010);
    }
    
	@Test
	public void test_14_createDir_more_suffix()
    {
 	   int Ret;
	   log.info( "test_14_createDir_more_suffix" );
	   Ret=tfsManager.createDir(appId, userId, "///textcreate14ms///");
	   Assert.assertEquals("Create Dir with right path should be true", Ret,0);
	   tfsManager.rmDir(appId, userId, "/textcreate14ms");
    }
	@Test
	public  void test_15_createDir_empty()
    {
 	   int Ret;
	   log.info( "test_14_createDir_more_suffix" );
	   Ret=tfsManager.createDir(appId, userId, "/    ");
	   Assert.assertEquals("Create Dir with right path should be false", Ret,-14010);
    }
      
}
