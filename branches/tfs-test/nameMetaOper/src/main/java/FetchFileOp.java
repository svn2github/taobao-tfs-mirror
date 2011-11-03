package com.taobao.common.tfs.nameMetaOper;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;
import com.taobao.common.tfs.DefaultTfsManager;

class FetchFileOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  String fetchedFile;
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  FetchFileOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_fetch_file";
    inputFile = "oper_save_file.fileList." + userId;
    fetchedFile = operConf.getPropValue(CONF_FETCH_FILE, "fetchedFile", "/home/admin/workspace/nameMetaOper/fetchedFile");
    statCount = Integer.parseInt(operConf.getPropValue(CONF_FETCH_FILE, "statCount", "1000"));
  }

  @Override
  void execute() { 
    readFromInputFile(inputFile, inputList);
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start fetch file operation ==" + tid + "== thread");
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
      int crcOrig = Integer.parseInt(tmp[4]);
      startTime = System.nanoTime();
      ret = tfsManager.fetchFile(appId, userId, fetchedFile, filePath);
      operTime = System.nanoTime() - startTime;
      if (ret) {
        int crcFetched = getCrc(fetchedFile);
        ret = (crcOrig == crcFetched ? true : false);
      }
      log.debug("@@ fetchFile appId: " + appId + ", userId: " + userId + ", filePath: " + filePath + (ret ? " success": " failed"));
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
