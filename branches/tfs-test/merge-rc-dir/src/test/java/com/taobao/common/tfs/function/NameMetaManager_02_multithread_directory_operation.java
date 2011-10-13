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

import com.taobao.common.tfs.function.tfsNameBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.namemeta.NameMetaManager;
public class NameMetaManager_02_multithread_directory_operation extends  tfsNameBaseCase{

	public static List<FileMetaInfo> file_info = null;
	public static boolean suc_flag=false;
  public int count = 10;
  public static List<String> nameMetaServerAddrList = new ArrayList<String>();
  @BeforeClass
    public  static void setUp() throws Exception {

	 	// tfsManager = (NameMetaManagerLite)appContext.getBean("tfsManager1");
		// tfsManager.init();
		 
	 //	 Assert.assertTrue(tfsManager.createDir("/public"));
	}
	@AfterClass
	public static void tearDown() throws Exception 
	{
	//	tfsManager.rmDir("/public");
	}
	
	@Test
	public void test_01_multi_threads_create_different_directories() throws Throwable {

		TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
		tr1 = new Thread1();
		tr2 = new Thread1();
		tr3 = new Thread1();
		tr4 = new Thread1();
		tr5 = new Thread1();
		tr6 = new Thread1();
		TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	@Test
	public void test_02_multi_threads_rename_differernt_directories_in_same_dir() throws Throwable {

		TestRunnable tr1 ,tr2, tr3;
		tr1 = new Thread2();
		tr2 = new Thread2();
		tr3 = new Thread2();
		TestRunnable[] trs = { tr1,tr2,tr3};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	@Test
	public void test_03_multi_threads_move_differernt_directories_in_same_dir() throws Throwable {

		TestRunnable tr1 ,tr2, tr3;
		tr1 = new Thread3();
		tr2 = new Thread3();
		tr3 = new Thread3();

		TestRunnable[] trs = {tr1,tr2,tr3};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
  
	@Test
	public void test_04_multi_threads_delete_different_directories() throws Throwable {

		TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
		tr1 = new Thread4();
		tr2 = new Thread4();
		tr3 = new Thread4();
		tr4 = new Thread4();
		tr5 = new Thread4();
		tr6 = new Thread4();
		TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
    
	@Test
	public void test_05_multi_threads_create_and_delete_one_directory() throws Throwable {

		TestRunnable tr1 ,tr2;
		tr1 = new Thread5();
		tr2 = new Thread5();
	
		TestRunnable[] trs = {tr1,tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
    
	@Test
	public void test_06_multi_threads_move_one_directory() throws Throwable {

		TestRunnable tr1 ,tr2;
		tr1 = new Thread6();
		tr2 = new Thread6();
	
		TestRunnable[] trs = {tr1,tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	@Test
	public void test_07_multi_threads_rename_one_directory() throws Throwable {

		TestRunnable tr1 ,tr2;
		tr1 = new Thread7();
		tr2 = new Thread7();
	
		TestRunnable[] trs = {tr1,tr2};
		MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
		mttr.runTestRunnables();
	}
	
	//Threads for every test case to call
  private class Thread1 extends TestRunnable {
    public long count=1000;
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        log.info("线程===" + Thread.currentThread().getId() + "执行开始");
        NameMetaManager tfsManager = new NameMetaManager();
        boolean ret;
        if(nameMetaServerAddrList.isEmpty())
        {
          nameMetaServerAddrList.add("10.232.36.208:5100");
        }
        // tfsManager.setAppId(Thread.currentThread().getId());
        tfsManager.setUserId(10);
        tfsManager.setNameMetaServerAddrList(nameMetaServerAddrList);
        tfsManager.setNsAddr("10.232.36.206:3100");
        tfsManager.setTimeout(3000);
        tfsManager.init();
        while(--count>0){
          ret = tfsManager.createDir("/"+String.valueOf(count));
          Assert.assertTrue(ret);
        }

      }
  }
  private class Thread2 extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        rename_different_directories();
      }
  }
  private class Thread3 extends TestRunnable {

    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        move_different_directories();
      }
  }
  private class Thread4 extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        delete_different_directories();
      }
  }
  private class Thread5 extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        create_and_delete_one_directory();
      }
  }
  private class Thread6 extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        move_one_directory();
      }
  }
  private class Thread7 extends TestRunnable {
    @Override
      public void runTest() throws Throwable {
        //线程要调用的方法或者要执行的操作
        rename_one_directory();
      }
  }


  public void create_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    NameMetaManager tfsManager = new NameMetaManager();
    boolean ret;
    if(nameMetaServerAddrList.isEmpty())
    {
      nameMetaServerAddrList.add("10.232.36.208:5100");
    }

    tfsManager.init();
    while(--count>0){
      ret = tfsManager.createDir(appId, userId,"/"+String.valueOf(1));
      Assert.assertTrue(ret);
    }


    // ret = tfsManager.rmDir("/"+String.valueOf(count));
    // Assert.assertTrue(ret);
    /* long temp = Thread.currentThread().getId();

       String filepath =String.valueOf(temp);

       log.info("filepath"+filepath);

       ret = tfsManager.createDir("/"+filepath);
       Assert.assertTrue(ret);

       ret = tfsManager.createDir("/"+filepath+"/newdir");
       Assert.assertTrue(ret);

       ret = tfsManager.rmDir("/"+filepath+"/newdir");
       Assert.assertTrue(ret);

       ret = tfsManager.rmDir("/"+filepath);
       Assert.assertTrue(ret);*/

  }
  public void create_and_delete_one_directory() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    //cannot verify the result of every single step ,do that in the last step
    tfsManager.createDir(appId, userId,"/bus");

    tfsManager.createDir(appId, userId,"/bus/test");

    tfsManager.createDir(appId, userId,"/bus/test/bike");

    tfsManager.mvDir(userId,"/bus/test/bike","/bus/test/rebike");

    tfsManager.createDir(appId, userId,"/bus/test/rebike");

    tfsManager.createDir(appId, userId,"/bus/test");

    tfsManager.createDir(appId, userId,"/bus");


  }
  public void rename_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    boolean ret;

    long temp = Thread.currentThread().getId();

    String filepath =String.valueOf(temp);
    log.info("filepath"+filepath);

    ret = tfsManager.createDir(appId, userId,"/"+filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/newdir");
    Assert.assertTrue(ret);


    ret = tfsManager.mvDir(userId,"/"+filepath+"/newdir", "/"+filepath+"/renamed");
    Assert.assertTrue(ret);


    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/renamed");
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath);
    Assert.assertTrue(ret);

  }
  public void rename_one_directory() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");

    tfsManager.createDir(appId, userId,"/public/test");

    tfsManager.mvDir(userId,"/public/test","/public/retest");

    tfsManager.createDir(appId, userId,"/public/retest");




  }
  public void move_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    boolean ret;

    long temp = Thread.currentThread().getId();

    String filepath =String.valueOf(temp);

    ret = tfsManager.createDir(appId, userId,"/public/"+filepath);
    Assert.assertTrue(ret);

    //	 ret = tfsManager.createDir("/move/"+filepath+"/"+filepath);
    //   Assert.assertTrue(ret);

    ret= tfsManager.mvDir(userId,"/public/"+filepath, "/"+filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath);
    Assert.assertTrue(ret);   
  }
  public void move_one_directory() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    boolean ret;

    ret = tfsManager.createDir(appId, userId,"/public/test");

    ret=  tfsManager.mvDir(userId,"/public/test", "/test");

    ret = tfsManager.createDir(appId, userId,"/test");

  }
  public void delete_different_directories() throws Exception 
  {    	 
    log.info("线程===" + Thread.currentThread().getId() + "执行开始");
    boolean ret;

    long temp = Thread.currentThread().getId();

    String filepath =String.valueOf(temp);

    log.info("filepath"+filepath);

    ret = tfsManager.createDir(appId, userId,"/"+filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/newdir");
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/newdir/"+filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/newdir/"+filepath);
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath+"/newdir");
    Assert.assertTrue(ret);

    ret = tfsManager.createDir(appId, userId,"/"+filepath);
    Assert.assertTrue(ret);

  }

}
