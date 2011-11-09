package com.taobao.common.tfs.nameMetaOper;

import java.util.List;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 

import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

class RmDirOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  RmDirOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_rm_dir";
    inputFile = "oper_create_dir.fileList." + userId;
    statCount = Integer.parseInt(operConf.getPropValue(CONF_RM_DIR, "statCount", "1000"));
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);

    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start rm dir operation ==" + tid + "== thread");
    boolean ret = false;

    for (int i = inputList.size()-1; i >= 0; i--) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != DIR_LIST_COL_COUNT) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String dirPath = tmp[2];
      startTime = System.nanoTime();
      ret = tfsManager.rmDir(appId, userId, dirPath);
      operTime = System.nanoTime() - startTime;
      log.debug("@@ rm appId: " + appId + ", userId: " + userId + ", dirPath: " + dirPath + (ret ? " success": " failed"));
      addStatInfo(statInfo, ret, operTime);
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
