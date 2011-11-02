package com.taobao.common.tfs.nameMetaOper;

import java.util.List;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 

import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

class LsDirOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  List<FileMetaInfo> fileMetaInfoList = null;
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  LsDirOp(long userId, DefaultTfsManager tfsManager) {
     super(userId, tfsManager);
     this.operType = "oper_ls_dir";
     inputFile = "oper_create_dir.fileList." + userId;
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);

    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start ls dir operation ==" + tid + "== thread");
    boolean ret = false;

    for (int i = 0; i < inputList.size(); i++) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != 4) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String dirPath = tmp[2];
      startTime = System.nanoTime();
      fileMetaInfoList = tfsManager.lsDir(appId, userId, dirPath);
      operTime = System.nanoTime() - startTime;
      ret = (fileMetaInfoList == null) ? false : true; 
      log.debug("@@ ls appId: " + appId + ", userId: " + userId + ", dirPath: " + dirPath + (ret ? " success": " failed"));
      addStatInfo(statInfo, ret, operTime);
      if (statInfo.totalCount % 100 == 0) {
        myLock.writeLock().lock(); 
        doStat(statInfo, opStatInfo);
        myLock.writeLock().unlock();
      }
    }
    myLock.writeLock().lock(); 
    doStat(statInfo, opStatInfo);
    myLock.writeLock().unlock();
  }
}
