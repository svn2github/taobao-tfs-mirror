package com.taobao.common.tfs.nameMetaOper;

import java.util.List;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 

import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

class MvDirOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  MvDirOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_mv_dir";
    inputFile = "oper_create_dir.fileList." + userId;
    statCount = Integer.parseInt(operConf.getPropValue(CONF_MV_DIR, "statCount", "1000"));
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);

    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start mv dir operation ==" + tid + "== thread");
    boolean ret = false;

    for (int i = inputList.size() - 1; i >= 0; i--) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != DIR_LIST_COL_COUNT) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String dirPath = tmp[2];
      String newDirPath = dirPath + "_mvd"; 
      startTime = System.nanoTime();
      ret = tfsManager.mvDir(appId, userId, dirPath, newDirPath);
      operTime = System.nanoTime() - startTime;
      log.debug("@@ mv appId: " + appId + ", userId: " + userId + ", dirPath: " + dirPath + " to " + newDirPath + (ret ? " success": " failed"));
      addStatInfo(statInfo, ret, operTime);
      // remove back
      ret = tfsManager.mvDir(appId, userId, newDirPath, dirPath);
      if (statInfo.totalCount % statCount == 0) {
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
