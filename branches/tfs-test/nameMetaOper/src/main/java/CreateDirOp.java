package com.taobao.common.tfs.nameMetaOper;

import java.io.FileWriter;
import java.io.BufferedWriter;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;
import com.taobao.common.tfs.DefaultTfsManager;

class CreateDirOp extends Operation { 

  private static StatInfo opStatInfo = new StatInfo();
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);
  private static boolean autoGenDir = true;
  private static int statCount = 1000;
 
  CreateDirOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_create_dir";
    String tmp = operConf.getPropValue(CONF_CREATE_DIR, "autoGenDir", "true");
    autoGenDir = tmp.equals("true") ? true : false;
    statCount = Integer.parseInt(operConf.getPropValue(CONF_CREATE_DIR, "statCount", "1000"));
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start create dir operation ==" + tid + "== thread");
    boolean ret = false;
    if (autoGenDir) {
      outputFile = operType + ".fileList." + userId;
      BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          startTime = System.nanoTime();
          ret = tfsManager.createDir(appId, userId, currDirPath);
          operTime = System.nanoTime() - startTime;
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
          addStatInfo(statInfo, ret, operTime);
          if (ret) { // success
            String record = appId + " " + userId + " " + currDirPath + " " + Path.getBaseName(currDirPath);
            outputList.add(record); 
          }
          if (statInfo.totalCount % statCount == 0) {
            for (int i = 0; i < outputList.size(); i++) {
              buffWriter.write(outputList.get(i));
              buffWriter.newLine();
            }
            buffWriter.flush();
            outputList.clear();
            myLock.writeLock().lock(); 
            doStat(statInfo, opStatInfo);
            myLock.writeLock().unlock();
          }
          genNextDirPath();
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          for (int i = 0; i < outputList.size(); i++) {
            buffWriter.write(outputList.get(i));
            buffWriter.newLine();
          }
          buffWriter.flush();
          buffWriter.close();
          outputList.clear();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    else {  // create the given dir list
      inputFile = "oper_create_dir.fileList." + userId;
      readFromInputFile(inputFile, inputList);

      for (int i = 0; i < inputList.size(); i++) {
        startTime = System.nanoTime();
        ret = tfsManager.createDir(appId, userId, inputList.get(i));
        operTime = System.nanoTime() - startTime;
        log.debug("@@ create Dir dirPath: " + inputList.get(i) + (ret ? " success": " failed"));
        addStatInfo(statInfo, ret, operTime);
        if (statInfo.totalCount % 5 == 0) {
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

}
