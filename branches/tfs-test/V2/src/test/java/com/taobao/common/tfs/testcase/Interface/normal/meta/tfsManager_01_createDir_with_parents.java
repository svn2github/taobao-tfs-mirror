package com.taobao.common.tfs.testcase.Interface.normal.meta;

import org.junit.Test;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;

import junit.framework.Assert;



public class tfsManager_01_createDir_with_parents extends metaTfsBaseCase
{
    @Test
    public void test_01_createDirWithParents_right_filePath()
    {
        log.info( "<---------- test_01_createDir_right_filePath ---------->" );
        int Ret;
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, "/testCDWP01/testCDWP1/testCDWP2/testCDWP3");
        Assert.assertEquals(0, iRet);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP01/testCDWP1/testCDWP2/testCDWP3");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP01/testCDWP1/testCDWP2");
        Assert.assertEquals(Ret,0);
        
        Ret = tfsManager.rmDir(appId, userId, "/testCDWP01/testCDWP1");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP01");
        Assert.assertEquals(Ret,0);
    }
    @Test
    public void test_02_createDirWithParents_wrong_filePath_01()
    {
        log.info("<----------- test_02_createDirWithParents_wrong_filePath_01 ------------------->");
        int iRet = -1;
        
        iRet = tfsManager.createDirWithParents(appId, userId, "testCDWP02");
        Assert.assertFalse("create dir with wrong filepath should be not zero", (0 == iRet) );
    }
    
    @Test
    public void test_03_createDirWithParents_wrong_filePath_02()
    {
        log.info("<----------- test_03_createDirWithParents_wrong_filePath_02 ------------------->");
        int iRet = -1;
    
        iRet = tfsManager.createDirWithParents(appId, userId, "testCDWP03/testCDWP");
        Assert.assertFalse("create dir with wrong filepath should be not zero", (0 == iRet) );
    }
    
    @Test
    public void test_04_createDirWithParents_wrong_filePath_03()
    {
        log.info("<----------- test_04_createDirWithParents_wrong_filePath_03 ------------------->");
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, "////testCDWP04///testCDWP////");
        
        Assert.assertEquals(0, iRet);
//        System.out.println("createDirWithParents wrong filePaht 03 return: " + iRet);
//        Assert.assertFalse("create dir with wrong filepath should be not zero", (0 == iRet) );
        int Ret;

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP04/testCDWP");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP04");
        Assert.assertEquals(Ret,0);

    }

    @Test
    public void test_05_createDirWithParents_wrong_filePath_04()
    {
        log.info("<----------- test_05_createDirWithParents_wrong_filePath_04 ------------------->");
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, "///////");
        Assert.assertFalse("create dir with wrong filepath should be not zero", (iRet == 0) );
    }
    
    @Test
    public void test_06_createDirWithParents_empty_filePath()
    {
        log.info("<----------- test_06_createDirWithParents_empty_filePath ------------------->");
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, "");
        Assert.assertFalse("create dir with wrong filepath should be not zero", (0 == iRet) );
    }

    @Test
    public void test_07_createDirWithParents_null_filePath()
    {
        log.info("<----------- test_07_createDirWithParents_null_filePath ------------------->");
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, null);
        Assert.assertFalse("create dir with wrong filepath should be not zero", (0 == iRet) );
    }

    @Test
    public void test_08_createDirWithParents_parents_exsit()
    {
        log.info("<--------------- test_08_createDirWithParents_parents_exsit ------------------>");
        int Ret;
        int iRet = -1;

        Ret = tfsManager.createDir(appId, userId, "/testCDWP08");
        Assert.assertEquals(Ret,0);
        
        Ret = tfsManager.createDir(appId, userId, "/testCDWP08/testCDWP108");
        Assert.assertEquals(Ret,0);

        iRet = tfsManager.createDirWithParents(appId, userId, "/testCDWP08/testCDWP108/testCDWP");
        Assert.assertTrue("Create Dir with parents exsit should be zero", (0 == iRet) );

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP08/testCDWP108/testCDWP");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP08/testCDWP108");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP08");
        Assert.assertEquals(Ret,0);
    }

    @Test
    public void test_09_createDirWithParents_some_parents_exsit()
    {
        log.info("<--------------- test_09_createDirWithParents_some_parents_exsit ------------------>");
        int Ret;
        int iRet = -1;
        
        Ret = tfsManager.createDir(appId, userId, "/testCDWP09");
        Assert.assertEquals(Ret,0);
    
        iRet = tfsManager.createDirWithParents(appId, userId, "/testCDWP09/testCDWP109/testCDWP");
        Assert.assertTrue("Create Dir with some parents exsit should be zero", (0 == iRet) );

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP09/testCDWP109/testCDWP");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP09/testCDWP109");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP09");
        Assert.assertEquals(Ret,0);
    }

    @Test
    public void test_10_createDirWithParents_twice_filePath()
    {
        log.info("<--------------- test_09_createDirWithParents_twice_filePath ------------------>");
        int Ret;
        int iRet = -1;

        iRet = tfsManager.createDirWithParents(appId, userId, "/testCDWP10/testCDWP");
        Assert.assertTrue("Create Dir should be zero", (0 == iRet) );

        iRet = tfsManager.createDirWithParents(appId, userId, "/testCDWP10/testCDWP");
        Assert.assertTrue("Create Dir with twice should be not zero", (0 != iRet) );

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP10/testCDWP");
        Assert.assertEquals(Ret,0);

        Ret = tfsManager.rmDir(appId, userId, "/testCDWP10");
        Assert.assertEquals(Ret,0);
   }
   @Test
   public void test_11_createDirWithParents_filepath_length_more_512()
   {
        log.info("<-------------- test_11_createDirWithParents_filepath_length_more_512 ------------------>");
        int iRet = -1;
        String s = new String();

        for(int i = 0;i < 512;i ++)
        {
            int a = 'a' + i%26;
            s += (char)a;
        }
        s ="/"+s;
        
        iRet = tfsManager.createDirWithParents(appId, userId, s);
        Assert.assertTrue("Create Dir with filepath length more 512 should be not zero", (0 != iRet) );
   }
   
}
