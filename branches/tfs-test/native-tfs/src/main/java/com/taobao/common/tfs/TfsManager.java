/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs;

import java.io.OutputStream;
import java.util.List;

import com.taobao.common.tfs.packet.FileInfo;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public interface TfsManager {

    /********************************
     *     
     *     Rc interface
     *
     *******************************/

    /**
     * save a local file to tfs
     *
     * @param localFileName
     * @param tfsFileName
     * @param tfsSuffix
     * @return  tfsfilename if save successully, or null if fail
     */
    public String saveFile(String localFileName, String tfsFileName, String tfsSuffix);

    /**
     * save a local file to tfs, return simple tfs name(no suffix encode)
     *
     * @param localFileName
     * @param tfsFileName
     * @param tfsSuffix
     * @param simpleName
     * @return  tfsfilename if save successully, or null if fail
     */
    public String saveFile(String localFileName, String tfsFileName, String tfsSuffix, boolean simpleName);

    /**
     * save local file(byte[]) to tfs
     *
     * @param tfsFileName
     * @param tfsSuffix
     * @param data   data to save
     * @param offset the start offset of data
     * @param length the size to save (ensure offset+length < data.length)
     * @return tfsfilename if save successully, or null if fail
     */
    public String saveFile(String tfsFileName, String tfsSuffix, byte[] data, int offset, int length);

    /**
     * save local file(byte[]) to tfs, return simple tfs name
     *
     * @param tfsFileName
     * @param tfsSuffix
     * @param data   data to save
     * @param offset the start offset of data
     * @param length the size to save (ensure offset+length < data.length)
     * @param simpleName
     * @return tfsfilename if save successully, or null if fail
     */
    public String saveFile(String tfsFileName, String tfsSuffix, byte[] data, int offset, int length, boolean simpleName);

    /**
     * save local file(byte[]) to tfs
     *
     * @param data  data to save
     * @param tfsFileName
     * @param tfsSuffix
     * @return      tfsfilename if save successully, or null if fail
     */
    public String saveFile(byte[] data, String tfsFileName, String tfsSuffix);

    /**
     * save local file(byte[]) to tfs (weird interface, reserve for compatibility)
     * return simple tfs name
     *
     * @param data  data to save
     * @param tfsFileName
     * @param tfsSuffix
     * @param simpleName
     * @return      tfsfilename if save successully, or null if fail
     */
    public String saveFile(byte[] data, String tfsFileName, String tfsSuffix, boolean simpleName);

    /**
     * save a local file to tfs large file anyway
     * @param localFileName
     * @param tfsFileName
     * @param tfsSuffix
     * @return
     */
    public String saveLargeFile(String localFileName, String tfsFileName, String tfsSuffix);

    /**
     * save a local file to tfs large file anyway
     * @param data
     * @param tfsFileName
     * @param tfsSuffix
     * @param key key name
     * @return
     */
    public String saveLargeFile(byte[] data, String tfsFileName, String tfsSuffix, String key);

    /**
     * save a local file to tfs large file anyway
     * @param tfsFileName
     * @param tfsSuffix
     * @param data   data to save
     * @param offset the start offset of data
     * @param length the size to save (ensure offset+length < data.length)
     * @param key key name
     * @return
     */
    public String saveLargeFile(String tfsFileName, String tfsSuffix, byte[] data, int offset, int length, String key);

    /**
     * stat a tfs file
     *
     * @param tfsFileName
     * @param tfsSuffix
     * @return
     */
    public FileInfo statFile(String tfsFileName, String tfsSuffix);


    /**
     * do action on a tfs file 
     *
     * @param tfsFileName
     * @param tfsSuffix
     * @param action 0: detele, 2: undelete, 4: conceal, 6: reveal
     * @return  true if delete successully, or false if fail
     */
    public boolean unlinkFile(String tfsFileName, String tfsSuffix, int action);

    /**
     * delete a tfs file
     *
     * @param tfsFileName
     * @param tfsSuffix
     * @return  true if delete successully, or false if fail
     */
    public boolean unlinkFile(String tfsFileName, String tfsSuffix);

    /**
     * hide file
     * @param tfsFileName
     * @param tfsSuffix
     * @param option 1 conceal 0 reveal
     * @return
     */
    public boolean hideFile(String fileName, String tfsSuffix, int option);

    /**
     * fetch a tfsfile to local disk
     *
     * @param tfsFileName (tfsFileName + tfsSuffix) = tfsName
     * @param tfsSuffix
     * @param localFileName
     * @return
     */
    public boolean fetchFile(String tfsFileName, String tfsSuffix, String localFileName);

    /**
     * fetch a tfsfile to output stream
     * @param tfsFileName
     * @param tfsSuffix
     * @param output
     * @return
     */
    public boolean fetchFile(String tfsFileName, String tfsSuffix, OutputStream output);

    /**
     * fetch a tfsfile to output stream
     * @param tfsFileName
     * @param tfsSuffix
     * @param offset offset of tfsfile to fetch (read until end)
     * @param output
     * @return
     */
    public boolean fetchFile(String tfsFileName, String tfsSuffix, long offset, OutputStream output);

    /**
     * fetch a tfsfile to output stream
     * @param tfsFileName
     * @param tfsSuffix
     * @param offset offset of tfsfile to fetch
     * @param length
     * @param output
     * @return
     */
    public boolean fetchFile(String tfsFileName, String tfsSuffix, long offset, long length, OutputStream output);

    /**************************************
     *                                    *
     *  tfs large file stream interface   *
     *                                    *
     **************************************/
    /**
     * open a tfs file to write
     * @param tfsFileName
     * @param tfsSuffix
     * @param key
     * @return
     */
    public int openWriteFile(String tfsFileName, String tfsSuffix, String key);

    /**
     * open a tfs file to read
     * @param tfsFileName
     * @param tfsSuffix
     * @return
     */
    public int openReadFile(String tfsFileName, String tfsSuffix);

    /**
     * read data
     * @param fd
     * @param data
     * @param offset
     * @param length
     * @return
     */
    public int readFile(int fd, byte[] data, int offset, int length);

    /**
     * read data
     * @param fd
     * @param fileOffset
     * @param data
     * @param offset
     * @param length
     * @return
     */
    public int readFile(int fd, long fileOffset, byte[] data, int offset, int length);

    /**
     * write data
     * @param fd
     * @param data
     * @param offset
     * @param length
     * @return
     */
    public int writeFile(int fd, byte[] data, int offset, int length);

    /**
     * close a tfs file and return a tfs file name
     *
     * @param fd
     * @return
     */
    public String closeFile(int fd);


    /********************************
     *     
     *     Namemeta interface
     *
     *******************************/

    /**
     * create a directory
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full directory path
     * @return true if success, else false
     */
    public boolean createDir(long userId, String filePath);

    /**
     * move a directory
     *
     * @param appId     appId
     * @param userId    userId
     * @param srcFilePath  source full directory path
     * @param destFilePath destination full directory path
     * @return true if success, else false
     */
    public boolean mvDir(long userId, String srcFilePath, String destFilePath);

    /**
     * delete a directory
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full directory path
     * @return true if success, else false
     */
    public boolean rmDir(long userId, String filePath);

    /**
     * list a directory's content, NOT recursive
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full directory path
     * @return List of fileMetaInfo if success, else null.
     */
    public List<FileMetaInfo> lsDir(long userId, String filePath);


    /**
     * list child file/directory's FileMetaInfo, recursive based on @isRecursive
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full directory path
     * @param isRecursive whether list recursivly
     * @return List of FileMetaInfo if success, else null.
     */
    public List<FileMetaInfo> lsDir(long userId, String filePath, boolean isRecursive);

    /**
     * create a file
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @return true if success, else false.
     */
    public boolean createFile(long userId, String filePath);

    /**
     * move a file
     *
     * @param appId     appId
     * @param userId    userId
     * @param srcFilePath  source full file path
     * @param destFilePath destination full file path
     * @return true if success, else false
     */
    public boolean mvFile(long userId, String srcFilePath, String destFilePath);

    /**
     * delete a file
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @return true if success, else false.
     */
    public boolean rmFile(long userId, String filePath);

    /**
     * list FileMetaInfo
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @return List of FileMetaInfo if success, else null.
     */
    public FileMetaInfo lsFile(long userId, String filePath);

    /**
     * read at most @length @data of file from file's @fileOffset to @output
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @param fileOffset offset of file to read from
     * @param length    length reading at most
     * @return length readed successfully (<= length), < 0 if occur fail when reading
     */
    public long read(long userId, String filePath, long fileOffset, long length, OutputStream output);

    /**
     * APPEND write byte[] data to file, equal to: write(appId, userId, filePath, data, 0, data.length);
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @param data      data to write
     * @return length writed successfully (<= data.length), < 0 if occur fail when writing
     */
    public long write(long userId, String filePath, byte[] data);

    /**
     * APPEND write @length of byte[] @data from @dataOffset to file
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @param data      data to write
     * @param dataOffset offset of data to write from
     * @param legnth    legnth of data to write
     * @return length writed successfully (<= length), < 0 if occur fail when writing
     */
    public long write(long userId, String filePath, byte[] data, long dataOffset, long length);

    /**
     * write byte[] @data to file from file's @fileOffset
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @param fileOffset    file offset from which to write
     *                  (NOTE: offset = -1 means append write, equal to: write(appId, userId, filePath, data);
     * @param data      data to write
     * @return length writed successfully (<= data.length), < 0 if occur fail when writing
     */
    public long write(long userId, String filePath, long fileOffset, byte[] data);

    /**
     * write @length of byte[] data from @dataOffset to file from @fileOffset
     *
     * @param appId     appId
     * @param userId    userId
     * @param filePath  full file path
     * @param fileOffset file's offset from which to write
     * @param data      data to write
     * @param dataOffset offset of data to write from
     * @param legnth    legnth of data to write
     * @return length writed successfully (<= length), < 0 if occur fail when writing
     */
    public long write(long userId, String filePath, long fileOffset,
                      byte[] data, long dataOffset, long length);

    /**
     * save a local file to file
     *
     * @param appId     appId
     * @param userId    userId
     * @param localfile local file path
     * @param filePath  full file path
     * @return true if success, else false
     */
    public boolean saveFile(long userId, String localFile, String filePath);

    /**
     * fetch file to a local file
     *
     * @param appId     appId
     * @param userId    userId
     * @param localfile local file path
     * @param filePath  full file path
     * @return true if success, else false
     */
    public boolean fetchFile(long userId, String localFile, String filePath);

    /**
     * destroy
     * @return
     */
    public void destroy();
}
