package com.taobao.common.tfs.nameMetaOper;

import java.util.List;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 

import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

class MvFileOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  MvFileOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_mv_file";
    inputFile = "oper_save_file.fileList." + userId;
    statCount = Integer.parseInt(operConf.getPropValue(CONF_MV_FILE, "statCount", "1000"));
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);

    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start mv file operation ==" + tid + "== thread");
    boolean ret = false;

    for (int i = inputList.size() - 1; i >= 0; i--) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != FILE_LIST_COL_COUNT) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String filePath = tmp[2];
      String newFilePath = filePath + "_mvd"; 
      startTime = System.nanoTime();
      ret = tfsManager.mvFile(appId, userId, filePath, newFilePath);
      operTime = System.nanoTime() - startTime;
      log.debug("@@ mv appId: " + appId + ", userId: " + userId + ", filePath: " + filePath + " to " + newFilePath + (ret ? " success": " failed"));
      addStatInfo(statInfo, ret, operTime);
      // move back
      ret = tfsManager.mvFile(appId, userId, newFilePath, filePath);
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
