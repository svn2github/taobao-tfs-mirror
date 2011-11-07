package com.taobao.common.tfs.nameMetaOper;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

class LsFileOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  FileMetaInfo fileMetaInfo = null;
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  LsFileOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
      super(operConf, userId, tfsManager);
      this.operType = "oper_ls_file";
      inputFile = "oper_save_file.fileList." + userId;
      statCount = Integer.parseInt(operConf.getPropValue(CONF_LS_FILE, "statCount", "1000"));
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);
 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start ls file operation ==" + tid + "== thread");
    boolean ret = false;

    for (int i = 0; i < inputList.size(); i++) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != 5) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String filePath = tmp[2];
      startTime = System.nanoTime();
      fileMetaInfo = tfsManager.lsFile(appId, userId, filePath);
      operTime = System.nanoTime() - startTime;
      ret = (fileMetaInfo == null) ? false : true; 
      log.debug("@@ ls appId: " + appId + ", userId: " + userId + ", filePath: " + filePath + (ret ? " success": " failed"));
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
