package com.taobao.common.tfs.testcase.function.meta;


import java.util.ArrayList;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;


 

import org.junit.AfterClass;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.common.TfsConstant;


public class NameMetaManager_04_multithread_small_file_operation  extends  metaTfsBaseCase
{

    public static String rootDir = "/NameMetaTest4";
    public static String localFile = resourcesPath + "2M.jpg";

    @BeforeClass
    public static void setUpOnce() throws Exception 
    {
         tfsManager.createDir(appId, userId, rootDir);
    }

    @AfterClass
    public  static void tearDownOnce() throws Exception
    {
        rmDirRecursive(appId, userId, rootDir); 
    }

    public class OperationFunction
	{
  
		public void actuator(int command,String rootDir,String localFile)
		{
			switch(command)
			{
				case 0:create_different_files(rootDir,localFile);break;
				case 1:rename_different_files(rootDir,localFile);break;
				case 2:move_different_files(rootDir,localFile);break;
				case 3:create_one_file(rootDir,localFile);break;
				case 4:move_one_file(rootDir,localFile);break;
				case 5:rename_one_file(rootDir,localFile);break;
			}
		}

		public void create_different_files(String rootDir, String localFile)
		{    	 
			int iRet = -1;
			boolean bRet = false;
			String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
			String filepath = dirpath + "/file";

			iRet = tfsManager.createDir(appId, userId, dirpath);
			Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
			
			bRet = tfsManager.saveFile(appId, userId, localFile, filepath);
			Assert.assertTrue(bRet);
			
			iRet = tfsManager.rmFile(appId, userId, filepath);
			Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		        
			iRet = tfsManager.rmDir(appId, userId, dirpath);
			Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}
	    
		public void rename_different_files(String rootDir, String localFile)
	    {    	 
			int iRet = -1;
	      
	        String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
	        String filepath = dirpath + "/file";
	        String newfilepath = dirpath + "/renamed";

	        iRet = tfsManager.createDir(appId, userId, dirpath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.createFile(appId, userId, filepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.mvFile(appId, userId, filepath, newfilepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.rmFile(appId, userId, newfilepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.rmDir(appId, userId, dirpath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	    }	
	    
		public void move_different_files(String rootDir, String localFile) 
	    {
			int iRet = -1;

	        String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
	        String filepath = dirpath + "/" + String.valueOf(Thread.currentThread().getId());
	        String newfilepath = rootDir + "/moved_" + String.valueOf(Thread.currentThread().getId());

	        iRet = tfsManager.createDir(appId, userId, dirpath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.createFile(appId, userId, filepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.mvFile(appId, userId, filepath, newfilepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.rmFile(appId, userId, newfilepath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

	        iRet = tfsManager.rmDir(appId, userId, dirpath);
	        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
	    }
	    
	    public void create_one_file(String rootDir, String localFile)
	    {
	        String dirpath = rootDir + "/public";
	        String filepath = dirpath + "/file";

	        tfsManager.createDir(appId, userId, dirpath);

	        tfsManager.saveFile(appId, userId, localFile, filepath);

	        tfsManager.rmFile(appId, userId, filepath);

	        tfsManager.rmDir(appId, userId, dirpath);
	    }
	    
	    public void move_one_file(String rootDir, String localFile)
	    {

	        String dirpath = rootDir + "/public";
	        String filepath = dirpath + "/file";
	        String newfilepath = rootDir + "/moved_file";

	        tfsManager.createDir(appId, userId, dirpath);

	        tfsManager.saveFile(appId, userId, localFile, filepath);

	        tfsManager.mvFile(appId, userId, filepath, newfilepath);

	        tfsManager.rmFile(appId, userId, newfilepath);

	        tfsManager.rmDir(appId, userId, dirpath);
	    }
	    
	    public void rename_one_file(String rootDir, String localFile) 
	    {
		    String dirpath = rootDir + "/public";
	        String filepath = dirpath + "/file";
	        String newfilepath = dirpath + "/renamed_file";

	        tfsManager.createDir(appId, userId, dirpath);

	        tfsManager.saveFile(appId, userId, localFile, filepath);

	        tfsManager.mvFile(appId, userId, filepath, newfilepath);

	        tfsManager.rmFile(appId, userId, newfilepath);

	        tfsManager.rmDir(appId, userId, dirpath);
	    }
	    
	}
    
	class Task implements Callable<String>
	{
	    private int id;
	    private OperationFunction op;
	    private String rootDir;
	    private String localFile;

	    public Task(int id,String rootDir, String localFile)
	    {
	    	this.op = new OperationFunction();
	        this.id = id;
	        this.rootDir = rootDir;
	        this.localFile = localFile;
	    }

	    public String call() throws Exception
	    {
	    	op.actuator(id,rootDir,localFile);
	        return "result of Task " + id;
	    }
	}

	public void wait (ArrayList<Future<String>> results)
	{
		boolean wait = true;
		int i;
		while(wait)
		{
			wait = false;
			try 
			{
				Thread.sleep(1000L);
			} 
			catch (InterruptedException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(i=0;i<results.size();i++)
			{
				if(!results.get(i).isDone())
				{
					wait = true;
					break;
				}
			}
		}
	}
    @Test
    public void test_01_multi_threads_create_and_delete_different_small_files() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(0,rootDir,localFile)));
        }        
        wait(results);
    }

    @Test
    public void test_02_multi_threads_rename_differernt_files() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(1,rootDir,localFile)));
        }        
        wait(results);
    }

    @Test
    public void test_03_multi_threads_move_different_files_to_same_dir() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(2,rootDir,localFile)));
        }        
        wait(results);
    }

    @Test
    public void test_04_multi_threads_create_and_save_and_delete_one_file() 
    { 
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(3,rootDir,localFile)));
        }        
        wait(results);
    }

    @Test
    public void test_05_multi_threads_move_one_file()  
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(4,rootDir,localFile)));
        }        
        wait(results);
    }

    @Test
    public void test_06_multi_threads_rename_one_file() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(5,rootDir,localFile)));
        }        
        wait(results);
    }
}

