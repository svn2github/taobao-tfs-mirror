package com.taobao.common.tfs.nameMetaOper;

// path methods
class Path {
  public static String getBaseName(String filePath) {
      if (null == filePath) {
          return null;
      }
      String strSlash = "/";
      if (filePath.equals(strSlash)) {
          return filePath;
      }
      String tmpPath = filePath;
      if (filePath.endsWith(strSlash)) {
          tmpPath = filePath.substring(0, filePath.length() - 1);
      }
      int lastPos = filePath.lastIndexOf(strSlash);
      if (-1 == lastPos) {
          return null;
      }
      String baseName = null;
      baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
      return baseName;
  }

  public static String getParentDir(String filePath) {
    if (null == filePath) {
      return null;
    }
    String strSlash = "/";
    if (filePath.equals(strSlash)) {
      return filePath;
    }
    String tmpPath = filePath;
    if (filePath.endsWith(strSlash)) {
      tmpPath = filePath.substring(0, filePath.length() - 1);
    }
    int lastPos = filePath.lastIndexOf(strSlash);
    if (-1 == lastPos) {
      return null;
    }
    String parentDir = null;
    // parent is "/"
    if (0 == lastPos) {
      parentDir = strSlash;
    }
    else {
      parentDir = tmpPath.substring(0, lastPos);
    }
    return parentDir;
  }

  public static String join(String parentDir, String baseName) {
    if (null == parentDir || null == baseName) {
      return null;
    }
    String strSlash = "/";
    if (parentDir.endsWith(strSlash)) {
      return parentDir + baseName;
    }
    else {
      return parentDir + "/" + baseName;
    }
  }

}
