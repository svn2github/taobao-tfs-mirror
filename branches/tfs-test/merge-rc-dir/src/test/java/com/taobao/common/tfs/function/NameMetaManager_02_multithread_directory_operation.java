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

import com.taobao.common.tfs.function.NameMetaBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_02_multithread_directory_operation extends  NameMetaBaseCase{

	public static List<FileMetaInfo> file_info = null;
  public static boolean suc_flag = false;
  public int count = 10;
  public static String rootDir = "/NameMetaTest2";

  @BeforeClass
  public  static void setUpOnce() throws Exception {
    NameMetaBaseCase.setUpOnce();
    rmDirRecursive(appId, userId, rootDir); 
  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    NameMetaBaseCase.tearDownOnce();
    rmDirRecursive(appId, userId, rootDir); 
  }


	@Test
	public void test_01_multi_threads_create_different_directories() throws Throwable {
		TestRunnable tr1, tr2, tr3, tr4, tr5, tr6;
		tr1 = new ThreadCreateDir();
		tr2 = new ThreadCreateDir();
		tr3 = new ThreadCreateDir();
		tr4 = new ThreadCreateDir();
		tr5 = new ThreadCreateDir();
		tr6 = new ThreadCreateDir();

		TestRunnable[] trs = {tr1, tr2, tr3, tr4, tr5, tr6};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	//@Test
	public void test_02_multi_threads_rename_differernt_directories_in_same_dir() throws Throwable {
		TestRunnable tr1, tr2, tr3;
		tr1 = new ThreadRenameDirs();
		tr2 = new ThreadRenameDirs();
		tr3 = new ThreadRenameDirs();

		TestRunnable[] trs = {tr1, tr2, tr3};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	//@Test
	public void test_03_multi_threads_move_differernt_directories_in_same_dir() throws Throwable {
		TestRunnable tr1, tr2, tr3;
		tr1 = new ThreadMoveDirs();
		tr2 = new ThreadMoveDirs();
		tr3 = new ThreadMoveDirs();

		TestRunnable[] trs = {tr1, tr2, tr3};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
  
	//@Test
	public void test_04_multi_threads_delete_different_directories() throws Throwable {
		TestRunnable tr1, tr2, tr3, tr4, tr5, tr6;
		tr1 = new ThreadRmDirs();
		tr2 = new ThreadRmDirs();
		tr3 = new ThreadRmDirs();
		tr4 = new ThreadRmDirs();
		tr5 = new ThreadRmDirs();
		tr6 = new ThreadRmDirs();

		TestRunnable[] trs = {tr1, tr2, tr3, tr4, tr5, tr6};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
    
	//@Test
	public void test_05_multi_threads_create_and_delete_one_directory() throws Throwable {
		TestRunnable tr1, tr2;
		tr1 = new ThreadCreateRmDir();
		tr2 = new ThreadCreateRmDir();
	
		TestRunnable[] trs = {tr1, tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
    
	//@Test
	public void test_06_multi_threads_move_one_directory() throws Throwable {
		TestRunnable tr1, tr2;
		tr1 = new ThreadMoveOneDir();
		tr2 = new ThreadMoveOneDir();
	
		TestRunnable[] trs = {tr1, tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	//@Test
	public void test_07_multi_threads_rename_one_directory() throws Throwable {
		TestRunnable tr1, tr2;
		tr1 = new ThreadRenameOneDir();
		tr2 = new ThreadRenameOneDir();
	
		TestRunnable[] trs = {tr1, tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	//Threads for every test case to call
  private class ThreadCreateDir extends TestRunnable {
      public long count = 1000;
      @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        log.info("线程===" + Thread.currentThread().getId() + "执行开始");
        while (--count > 0) {
          boolean ret = false;
          ret = tfsManager.createDir(appId, userId, "/" + String.valueOf(count));
          Assert.assertTrue(ret);
        }
      }
  }
  private class ThreadRenameDirs extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        rename_different_directories();
      }
  }

  private class ThreadMoveDirs extends TestRunnable {

    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        move_different_directories();
      }
  }
  private class ThreadRmDirs extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        delete_different_directories();
      }
  }
  private class ThreadCreateRmDir extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        create_and_delete_one_directory();
      }
  }
  private class ThreadMoveOneDir extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        move_one_directory();
      }
  }
  private class ThreadRenameOneDir extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        rename_one_directory();
      }
  }

  public void create_and_delete_one_directory() throws Exception 
  {
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    String filepath = rootDir + "/public";
    String subDir = filepath + "/subdir";
    String suberDir = subDir + "/suberdir";

    tfsManager.createDir(appId, userId, filepath);

    tfsManager.createDir(appId, userId, subDir);

    tfsManager.createDir(appId, userId, suberDir);

    tfsManager.rmDir(appId, userId, suberDir);

    tfsManager.rmDir(appId, userId, subDir);

    tfsManager.rmDir(appId, userId, filepath);
  }

  public void rename_different_directories() throws Exception {
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    boolean ret = false;
    long threadId = Thread.currentThread().getId();
    String filepath = rootDir + "/" + String.valueOf(threadId);
    String subDir = filepath + "/subdir";
    String renamedDir = filepath + "/renamed";

    ret = tfsManager.createDir(appId, userId, filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId, subDir);
    Assert.assertTrue(ret);

    ret = tfsManager.mvDir(appId, userId, subDir, renamedDir);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, renamedDir);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath);
    Assert.assertTrue(ret);
  }

  public void rename_one_directory() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    String filepath = rootDir + "/public/test";
    String newpath = rootDir + "/public/renamed";

    tfsManager.createDir(appId, userId, filepath);

    tfsManager.mvDir(appId, userId, filepath, newpath);

    tfsManager.rmDir(appId, userId, newpath);
  }

  public void move_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    boolean ret = false;
    long threadId = Thread.currentThread().getId();
    String filepath = rootDir + "/public/" + String.valueOf(threadId);
    String newpath = rootDir + "/moved_" + String.valueOf(threadId);

    ret = tfsManager.createDir(appId, userId, filepath);
    Assert.assertTrue(ret);

    ret= tfsManager.mvDir(appId, userId, filepath, newpath);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, newpath);
    Assert.assertTrue(ret);   
  }

  public void move_one_directory() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    boolean ret = false;
    String filepath = rootDir + "/public/test";
    String newpath = rootDir + "/moved";

    ret = tfsManager.createDir(appId, userId, filepath);

    ret=  tfsManager.mvDir(appId, userId, filepath, newpath);

    ret = tfsManager.rmDir(appId, userId, newpath);
  }

  public void delete_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    boolean ret = false;
    long threadId = Thread.currentThread().getId();

    String filepath = rootDir + "/" + String.valueOf(threadId);

    ret = tfsManager.createDir(appId, userId, filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath);
    Assert.assertTrue(ret);
  }
}
