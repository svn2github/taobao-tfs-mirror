package com.taobao.common.tfs.nameMetaOper;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;
import com.taobao.common.tfs.DefaultTfsManager;

class MixOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  MixOp(long userId, DefaultTfsManager tfsManager) {
     super(userId, tfsManager);
     this.operType = "oper_mix";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    String localSmallFile = "1K.jpg"; 
    String retFile = "retFile"; 
    long tid = Thread.currentThread().getId();
    log.info("start mix operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;

    while (true) {
      count++;
      String dirPath = "/dir_" + String.valueOf(tid) + "_" + String.valueOf(count);
      startTime = System.nanoTime();
      ret = tfsManager.createDir(appId, userId, dirPath);
      operTime = System.nanoTime() - startTime;
      log.debug("@@ create Dir dirPath: " + dirPath + (ret ? "success": " failed"));
      addStatInfo(statInfo, ret, operTime);
      if (ret) {
        for (int i = 0; i < 500; i++) {
          String filePath = dirPath + "/file_" + String.valueOf(tid) + "_" + String.valueOf(i);
          startTime = System.nanoTime();
          ret = tfsManager.saveFile(appId, userId, localSmallFile, filePath);
          operTime = System.nanoTime() - startTime;
          log.debug("@@ save file: " + filePath + (ret ? "success": " failed"));
          addStatInfo(statInfo, ret, operTime);
          if (ret)
          {
            startTime = System.nanoTime();
            ret = tfsManager.fetchFile(appId, userId, retFile, filePath);
            operTime = System.nanoTime() - startTime;
            addStatInfo(statInfo, ret, operTime);
            startTime = System.nanoTime();
            ret = tfsManager.rmFile(appId, userId, filePath);
            operTime = System.nanoTime() - startTime;
            addStatInfo(statInfo, ret, operTime);
          }
        }
        startTime = System.nanoTime();
        ret = tfsManager.rmDir(appId, userId, dirPath);
        operTime = System.nanoTime() - startTime;
        addStatInfo(statInfo, ret, operTime);
      }
      if (statInfo.totalCount % 1000 == 0) {
        myLock.writeLock().lock(); 
        doStat(statInfo, opStatInfo);
        myLock.writeLock().unlock();
      }
      myLock.writeLock().unlock();
    }
  }
}
