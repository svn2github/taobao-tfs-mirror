package com.taobao.common.tfs.nameMetaOper;

import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;

public class nameMetaOper{

  // operation type
  final static int OPER_CREATE_DIR = 1;
  final static int OPER_LS_DIR = 2;
  final static int OPER_MV_DIR = 4;
  final static int OPER_RM_DIR = 8;
  final static int OPER_CREATE_FILE = 16;
  final static int OPER_LS_FILE = 32;
  final static int OPER_SAVE_SMALL_FILE = 64;
  final static int OPER_SAVE_LARGE_FILE = 128;
  final static int OPER_FETCH_FILE = 256;
  final static int OPER_READ = 512;
  final static int OPER_WRITE = 1024;
  final static int OPER_MV_FILE = 2048;
  final static int OPER_RM_FILE = 4096;
  final static int OPER_MIX = 8192;

  final static String CONF_PUBLIC = "public";

  public static void main(String[] args) { 

    if (args.length < 1) {
      System.out.println("usage: nameMetaOper configFile");
      System.exit(1);
    }
    String configFile = args[0];
    SectProp operConf = new SectProp();
    boolean bRet = operConf.load(configFile);
    if (false == bRet) {
      System.out.println("@@@ load config file: " + configFile + " error!");
      System.exit(1);
    }

    // create thread pool 
    int threadCount = Integer.parseInt(operConf.getPropValue(CONF_PUBLIC, "threadCount", "10"));
    int operType = Integer.parseInt(operConf.getPropValue(CONF_PUBLIC, "operType", "1"));
    int userId = Integer.parseInt(operConf.getPropValue(CONF_PUBLIC, "userId", "381"));
    ExecutorService pool = Executors.newFixedThreadPool(threadCount); 

    // init tfsManager
    ClassPathXmlApplicationContext clientFactory = new ClassPathXmlApplicationContext("tfs.xml");
    DefaultTfsManager tfsManager = (DefaultTfsManager) clientFactory.getBean("tfsManager");
 
    // start thread
    Operation oper = null;
    for (int i = 0; i < threadCount; i++) {
      switch (operType) {
        case OPER_CREATE_DIR:
          oper = new CreateDirOp(operConf, userId, tfsManager);
        break;
        case OPER_LS_DIR:
         oper = new LsDirOp(operConf, userId, tfsManager);
        break;
        case OPER_MV_DIR:
         oper = new MvDirOp(operConf, userId, tfsManager);
        break;
         case OPER_RM_DIR:
         oper = new RmDirOp(operConf, userId, tfsManager);
        break;
        case OPER_CREATE_FILE:
          oper = new CreateFileOp(operConf, userId, tfsManager);
        break;
        case OPER_SAVE_SMALL_FILE:
          oper = new SaveSmallFileOp(operConf, userId, tfsManager);
        break;
        case OPER_FETCH_FILE:
         oper = new FetchFileOp(operConf, userId, tfsManager);
        break;
        case OPER_SAVE_LARGE_FILE:
          oper = new SaveLargeFileOp(operConf, userId, tfsManager);
        break;
        case OPER_LS_FILE:
         oper = new LsFileOp(operConf, userId, tfsManager);
        break;
        case OPER_MV_FILE:
         oper = new MvFileOp(operConf, userId, tfsManager);
        break;
        case OPER_RM_FILE:
         oper = new RmFileOp(operConf, userId, tfsManager);
        break;
 
        case OPER_MIX:
          oper = new MixOp(operConf, userId, tfsManager);
        break;
        default:
      }

      pool.execute(oper); 
      userId++;
    }

    // close thread pool
    pool.shutdown(); 
    try {
      while (!pool.isTerminated()) {
        Thread.sleep(6000);
      } 
    } catch (InterruptedException ie) {
      pool.shutdownNow();
    } finally {
      tfsManager.destroy();
    }
  }

} 
