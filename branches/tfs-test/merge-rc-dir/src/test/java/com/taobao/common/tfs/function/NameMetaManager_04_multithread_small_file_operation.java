package com.taobao.common.tfs.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import net.sourceforge.groboutils.junit.v1.MultiThreadedTestRunner; 
import net.sourceforge.groboutils.junit.v1.TestRunnable;  
 
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.NameMetaManagerBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_04_multithread_small_file_operation  extends  NameMetaManagerBaseCase{

    public static String rootDir = "/NameMetaTest4";
    public static String localFile = resourcesPath + "/2M";

    @BeforeClass
    public static void setUpOnce() throws Exception {
        boolean bret = false;
        //rmDirRecursive(appId, userId, rootDir); 

        bret = tfsManager.createDir(appId, userId, rootDir);
        Assert.assertTrue(bret);
    }

    @AfterClass
    public  static void tearDownOnce() throws Exception {
        rmDirRecursive(appId, userId, rootDir); 
    }

    @Test
    public void test_01_multi_threads_create_and_delete_different_small_files() throws Throwable {
        TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
        tr1 = new ThreadCreateFiles();
        tr2 = new ThreadCreateFiles();
        tr3 = new ThreadCreateFiles();
        tr4 = new ThreadCreateFiles();
        tr5 = new ThreadCreateFiles();
        tr6 = new ThreadCreateFiles();
        TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }

    @Test
    public void test_02_multi_threads_rename_differernt_files() throws Throwable {
        TestRunnable tr1, tr2,tr3, tr4, tr5, tr6;
        tr1 = new ThreadRenameFiles();
        tr2 = new ThreadRenameFiles();
        tr3 = new ThreadRenameFiles();
        tr4 = new ThreadRenameFiles();
        tr5 = new ThreadRenameFiles();
        tr6 = new ThreadRenameFiles();
        TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }

    @Test
    public void test_03_multi_threads_move_different_files_to_same_dir() throws Throwable {
        TestRunnable tr1,tr2, tr3, tr4, tr5, tr6;
        tr1 = new ThreadMoveFiles();
        tr2 = new ThreadMoveFiles();
        tr3 = new ThreadMoveFiles();
        tr4 = new ThreadMoveFiles();
        tr5 = new ThreadMoveFiles();
        tr6 = new ThreadMoveFiles();
        TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }

    @Test
    public void test_04_multi_threads_create_and_save_and_delete_one_file() throws Throwable { 
        TestRunnable tr1, tr2, tr3, tr4, tr5, tr6;
        tr1 = new ThreadCreateOneFile();
        tr2 = new ThreadCreateOneFile();
        tr3 = new ThreadCreateOneFile();
        tr4 = new ThreadCreateOneFile();
        tr5 = new ThreadCreateOneFile();
        tr6 = new ThreadCreateOneFile();
        TestRunnable[] trs = {tr1 ,tr2, tr3,tr4 ,tr5, tr6};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }

    @Test
    public void test_05_multi_threads_move_one_file() throws Throwable {
        TestRunnable tr1, tr2, tr3, tr4, tr5, tr6;
        tr1 = new ThreadMoveOneFile();
        tr2 = new ThreadMoveOneFile();
        tr3 = new ThreadMoveOneFile();
        tr4 = new ThreadMoveOneFile();
        tr5 = new ThreadMoveOneFile();
        tr6 = new ThreadMoveOneFile();
        TestRunnable[] trs = {tr1 ,tr2, tr3,tr4 ,tr5, tr6};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }

    @Test
    public void test_06_multi_threads_rename_one_file() throws Throwable {
        TestRunnable tr1, tr2;
        tr1 = new ThreadRenameOneFile();
        tr2 = new ThreadRenameOneFile();

        TestRunnable[] trs = {tr1, tr2};
        MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
        mttr.runTestRunnables();
    }
	
    //Threads for every test case to call
    private class ThreadCreateFiles extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            create_different_files(rootDir, localFile);
        }
    }
  
    private class ThreadRenameFiles extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            rename_different_files(rootDir, localFile);
        }
    }
	
    private class ThreadMoveFiles extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            move_different_files(rootDir, localFile);
        }
    }

    private class ThreadCreateOneFile extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            create_one_file(rootDir, localFile);
        }
    }
   
    private class ThreadMoveOneFile extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            move_one_file(rootDir, localFile);
        }
    }
   
    private class ThreadRenameOneFile extends TestRunnable {
        @Override
        public void runTest() throws Throwable {
            //线程要调用的方法或者要执行的操作
            rename_one_file(rootDir, localFile);
        }
    }

}

