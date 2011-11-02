package com.taobao.common.tfs.nameMetaOper;

import java.io.FileWriter;
import java.io.BufferedWriter;

import java.util.concurrent.locks.ReentrantReadWriteLock; 
import java.util.concurrent.locks.ReadWriteLock; 
import org.apache.log4j.Logger;
import com.taobao.common.tfs.DefaultTfsManager;

class SaveSmallFileOp extends Operation { 
 
  private static StatInfo opStatInfo = new StatInfo();
  private static String localSmallFile = "/home/admin/workspace/nameMetaOper/100K";
  private static String smallFileName = "smallfile";
  private static int crc = 0;
  private static ReadWriteLock myLock = new ReentrantReadWriteLock(false);

  SaveSmallFileOp(long userId, DefaultTfsManager tfsManager) {
     super(userId, tfsManager);
     this.operType = "oper_save_small_file";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start save small file operation ==" + tid + "== thread");
    boolean ret = false;
    boolean autoGenDir = true;
    //boolean autoGenDir = false;

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
            currFilePath = currDirPath + "/" + smallFileName;
            startTime = System.nanoTime();
            ret = tfsManager.saveFile(appId, userId, localSmallFile, currFilePath);
            operTime = System.nanoTime() - startTime;
            log.debug("@@ save local file: " + localSmallFile + " to: " + currFilePath + (ret ? " success": " failed"));
            if (ret) {
              crc = getCrc(localSmallFile);
              String record = appId + " " + userId + " " + currFilePath + " " + Path.getBaseName(currFilePath) + " " + crc;
              outputList.add(record); 
            }
          }
          addStatInfo(statInfo, ret, operTime);
          if (statInfo.totalCount % 100 == 0) {
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
        ret = tfsManager.saveFile(appId, userId, localSmallFile, inputList.get(i));
        operTime = System.nanoTime() - startTime;
        log.debug("@@ save local file: " + localSmallFile + " to: " + inputList.get(i) + (ret ? " success": " failed"));
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
