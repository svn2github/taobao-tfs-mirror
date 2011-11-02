package com.taobao.common.tfs.nameMetaOper;

import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;

public class nameMetaOper{

  // operation type
  final static int OPER_CREATE_DIR = 1;
  final static int OPER_LS_DIR = 2;
  final static int OPER_SAVE_SMALL_FILE = 4;
  final static int OPER_SAVE_LARGE_FILE = 8;
  final static int OPER_FETCH_FILE = 16;
  final static int OPER_LS_FILE = 32;
  final static int OPER_REMOVE_FILE = 64;
  final static int OPER_MIX = 128;

  public static void main(String[] args) { 

    if (args.length < 1) {
      System.out.println("usage: nameMetaOper operType userId");
      System.exit(1);
    }
    int operType = Integer.parseInt(args[0]);
    long userId = 0;
    if (args.length == 2) {
      userId = Long.parseLong(args[1]);
    }

    // create thread pool 
    int operCount = 15; // TODO: read from conf
    ExecutorService pool = Executors.newFixedThreadPool(operCount); 

    // init tfsManager
    ClassPathXmlApplicationContext clientFactory = new ClassPathXmlApplicationContext("tfs.xml");
    DefaultTfsManager tfsManager = (DefaultTfsManager) clientFactory.getBean("tfsManager");
 
    // start thread
    Operation oper = null;
    for (int i = 0; i < operCount; i++) {
      switch (operType) {
        case OPER_CREATE_DIR:
          oper = new CreateDirOp(userId, tfsManager);
        break;
        case OPER_LS_DIR:
         oper = new LsDirOp(userId, tfsManager);
        break;
        case OPER_SAVE_SMALL_FILE:
          oper = new SaveSmallFileOp(userId, tfsManager);
        break;
        case OPER_FETCH_FILE:
         oper = new FetchFileOp(userId, tfsManager);
        break;
        case OPER_SAVE_LARGE_FILE:
          oper = new SaveLargeFileOp(userId, tfsManager);
        break;
        case OPER_LS_FILE:
         oper = new LsFileOp(userId, tfsManager);
        break;
        case OPER_MIX:
          oper = new MixOp(userId, tfsManager);
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
