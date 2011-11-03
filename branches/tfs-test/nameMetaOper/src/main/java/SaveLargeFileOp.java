package com.taobao.common.tfs.nameMetaOper;

import java.io.FileWriter;
import java.io.BufferedWriter;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;
import com.taobao.common.tfs.DefaultTfsManager;

class SaveLargeFileOp extends Operation { 
 
  private static StatInfo opStatInfo = new StatInfo();
  private static boolean autoGenDir = true;
  private static String localLargeFile;
  private static String largeFileName;
  private static int crc = 0;
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  SaveLargeFileOp(SectProp operConf, long userId, DefaultTfsManager tfsManager) {
    super(operConf, userId, tfsManager);
    this.operType = "oper_save_large_file";
    localLargeFile = operConf.getPropValue(CONF_SAVE_LARGE_FILE, "localLargeFile", "/home/admin/workspace/nameMetaOper/2.1G");
    largeFileName = operConf.getPropValue(CONF_SAVE_LARGE_FILE, "largeFileName", "largefile");
    String tmp = operConf.getPropValue(CONF_SAVE_LARGE_FILE, "autoGenDir", "true");
    autoGenDir = tmp.equals("true") ? true : false;
    statCount = Integer.parseInt(operConf.getPropValue(CONF_SAVE_LARGE_FILE, "statCount", "10"));
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start save large file operation ==" + tid + "== thread");
    boolean ret = false;

    if (autoGenDir) {
        outputFile = "oper_save_file.fileList." + userId;
        BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          startTime = System.nanoTime();
          ret = tfsManager.createDir(appId, userId, currDirPath);
          operTime = System.nanoTime() - startTime;
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
          if (ret) { // success
            currFilePath = currDirPath + "/" + largeFileName;
            startTime = System.nanoTime();
            ret = tfsManager.saveFile(appId, userId, localLargeFile, currFilePath);
            operTime = System.nanoTime() - startTime;
            log.debug("@@ save local file: " + localLargeFile + " to: " + currFilePath + (ret ? " success": " failed"));
            if (ret) {
              crc = getCrc(localLargeFile);
              String record = appId + " " + userId + " " + currFilePath + " " + Path.getBaseName(currFilePath) + " " + crc;
              outputList.add(record); 
            }
          }
          addStatInfo(statInfo, ret, operTime);
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
    else {
      inputFile = "oper_save_file.fileList." + userId;
      readFromInputFile(inputFile, inputList);

      for (int i = 0; i < inputList.size(); i++) {
        startTime = System.nanoTime();
        ret = tfsManager.saveFile(appId, userId, localLargeFile, inputList.get(i));
        operTime = System.nanoTime() - startTime;
        log.debug("@@ save local file: " + localLargeFile + " to: " + inputList.get(i) + (ret ? " success": " failed"));
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

}
