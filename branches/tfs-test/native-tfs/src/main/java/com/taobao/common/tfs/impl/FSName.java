/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.impl;

import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.taobao.common.tfs.TfsException;

public class FSName {
    public static final int MAIN_FILENAME_LENGTH = 18;
    // just for compatibility
    public static final int STANDARD_SUFFIX_LENGTH = 4;

    public static final char TFS_SMALL_KEY_CHAR = 'T';
    public static final char TFS_LARGE_KEY_CHAR = 'L';

    private static final int ENCODE_FILENAME_LENGTH = 16;
    private static final int ENCODE_FILECODE_LENGTH = 12;
    private static final int ENCODE_SIMPLECODE_LENGTH = 8;

    private char tfsClusterIndex;
    private byte[] fileCode;

    private static final byte[] tagTable = ("Taobao-inc").getBytes();
    private static final int tagLength = tagTable.length;
    private static final byte[] encodingTable =
        ("0JoU8EaN3xf19hIS2d.6pZRFBYurMDGw7K5m4CyXsbQjg_vTOAkcHVtzqWilnLPe").getBytes();
    private static final byte[] decodingTable = {
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,18,0,0,11,16,8,36,34,19,32,4,12,0,0,0,0,0,
        0,0,49,24,37,29,5,23,30,52,14,1,33,61,28,7,48,62,42,22,15,47,3,53,57,
        39,25,21,0,0,0,0,45,0,6,41,51,17,63,10,44,13,58,43,50,59,35,60,2,20,
        56,27,40,54,26,46,31,9,38,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
        ,0,0,0,0,0,0,0,0,0,0,0,0};

    public FSName(String tfsName) throws TfsException {
        tfsClusterIndex = '1';
        set(tfsName);
    }

    public FSName(int clusterId, int blockId, long fileId) {
        tfsClusterIndex = (char)('0' + clusterId);
        set(blockId, fileId);
    }

    public FSName(int blockId, long fileId) {
        tfsClusterIndex = '1';
        set(blockId, fileId);
    }

    public FSName(String mainFileName, String suffix) throws TfsException {
        tfsClusterIndex = '1';
        set(mainFileName, suffix);
    }

    private int check(String mainFileName, String suffix) {
        if (mainFileName == null || mainFileName.length() == 0) {
            return 0;
        }

        int length = mainFileName.length();
        if (length < MAIN_FILENAME_LENGTH ||
            (mainFileName.charAt(0) != TFS_SMALL_KEY_CHAR &&
             mainFileName.charAt(0) != TFS_LARGE_KEY_CHAR) ||
            (mainFileName.charAt(1) < '0' || mainFileName.charAt(1) > '9')) {
            return -1;
        }

        if (length > MAIN_FILENAME_LENGTH) { // mainname has suffix
            if (null == suffix || 0 == suffix.length()) { // no specified suffix, use mainname's
                return 2;
            }
            if (! mainFileName.substring(MAIN_FILENAME_LENGTH).equals(suffix)) {
                // has specified suffix, then mainname's must equal with specified
                return -1;
            }
        }

        return 1;
    }

    private byte[] xorTagCode(byte[] input) {
        int length = input.length;
        byte[] output = new byte[length];
        for (int i = 0; i < length; i++) {
            output[i] = (byte)(input[i] ^ tagTable[i%tagLength]);
        }
        return output;
    }

    private byte[] encode(byte[] fileId, boolean simple) {
        if (fileId.length != ENCODE_FILECODE_LENGTH) {
            return null;
        }

        byte[] fileIdX = fileId;

        if (simple) {
            fileIdX = new byte[fileId.length];
            System.arraycopy(fileId, 0, fileIdX, 0, fileId.length);
            // only encode blockid & seqid, no suffix
            Arrays.fill(fileIdX, ENCODE_SIMPLECODE_LENGTH, ENCODE_FILECODE_LENGTH, (byte)0x0);
        }
        fileIdX = xorTagCode(fileIdX);

        byte[] fileName = new byte[ENCODE_FILENAME_LENGTH];
        int i, k = 0;
        int x;
        for (i = 0; i < ENCODE_FILECODE_LENGTH; i += 3) {
            x = ((fileIdX[i] << 16) & 0xff0000)
                + ((fileIdX[i + 1] << 8) & 0xff00) + (fileIdX[i + 2] & 0xff);
            fileName[k++] = encodingTable[x >>> 18];
            fileName[k++] = encodingTable[(x >>> 12) & 0x3f];
            fileName[k++] = encodingTable[(x >>> 6) & 0x3f];
            fileName[k++] = encodingTable[x & 0x3f];
        }
        return fileName;
    }

    private byte[] decode(byte[] fileName) {
        if (fileName.length != ENCODE_FILENAME_LENGTH) {
            return null;
        }

        byte[] fileId = new byte[ENCODE_FILECODE_LENGTH];

        int i, k = 0;
        int x;
        for (i = 0; i < ENCODE_FILENAME_LENGTH; i += 4) {
            x = (decodingTable[fileName[i]&0xff] << 18) +
                (decodingTable[fileName[i+1]&0xff] << 12) +
                (decodingTable[fileName[i+2]&0xff] << 6) +
                decodingTable[fileName[i+3]&0xff];
            fileId[k++] = (byte)((x >>> 16) & 0xff);
            fileId[k++] = (byte)((x >>> 8) & 0xff);
            fileId[k++] = (byte)(x & 0xff);
        }
        return xorTagCode(fileId);
    }

    private int hash(String suffix) {
        if (suffix == null) {
            return 0x80000000;
        }
        int len = suffix.length();
        int h = 0, i;
        for(i=0; i<len; i++) {
            h += suffix.charAt(i);
            h *= 7;
        }
        return (h | 0x80000000);
    }

    private void setCode(int offset, int value) {
        ByteBuffer bf;
        if (this.fileCode == null) {
            bf = ByteBuffer.allocate(ENCODE_FILECODE_LENGTH) ;
        } else {
            bf = ByteBuffer.wrap(this.fileCode);
        }
        bf.order(ByteOrder.LITTLE_ENDIAN);
        bf.putInt(offset, value);
        this.fileCode = bf.array();
    }

    private int getCode(int offset) {
        if (this.fileCode == null) return 0;
        ByteBuffer bf = ByteBuffer.wrap(this.fileCode);
        bf.order(ByteOrder.LITTLE_ENDIAN);
        return bf.getInt(offset);
    }

    public void setBlockId(int blockId) {
        setCode(0, blockId);
    }

    public void setSeqId(int seqId) {
        setCode(4, seqId);
    }

    public void setSuffix(int suffix) {
        setCode(8, suffix);
    }

    public void setSuffix(String suffix) {
        if (suffix != null && suffix.length() > 0)
            setSuffix(hash(suffix));
    }

    public void setFileId(long fileId) {
        ByteBuffer bf = ByteBuffer.wrap(this.fileCode);
        bf.order(ByteOrder.LITTLE_ENDIAN);
        bf.putLong(4, fileId);
        this.fileCode = bf.array();
    }

    public int getBlockId() {
        return getCode(0);
    }

    public int getSeqId() {
        return getCode(4);
    }

    public int getSuffix() {
        return getCode(8);
    }

    public long getFileId() {
        if (this.fileCode == null) return 0;
        ByteBuffer bf = ByteBuffer.wrap(this.fileCode);
        bf.order(ByteOrder.LITTLE_ENDIAN);
        return bf.getLong(4);
    }

    private boolean set(String tfsName) throws TfsException {
        return set(tfsName, null);
    }

    private boolean set(String mainFileName, String suffix) throws TfsException {
        int ret = check(mainFileName, suffix);
        if (ret == -1) {
            throw new TfsException("illegal tfsname, name:"
                                   + mainFileName + ",suffix:" + suffix);
        } else if (ret == 0) {
            this.fileCode = new byte[ENCODE_FILECODE_LENGTH];
        } else {
            byte[] name = new byte[ENCODE_FILENAME_LENGTH];
            System.arraycopy(mainFileName.getBytes(), 2, name, 0,
                             ENCODE_FILENAME_LENGTH);
            this.fileCode = this.decode(name);

            if (ret == 2) {
                suffix = mainFileName.substring(MAIN_FILENAME_LENGTH);
            }
        }

        if (mainFileName != null && mainFileName.length() > 0) {
            tfsClusterIndex = mainFileName.charAt(1);
        }

        if (suffix != null && suffix.length() > 0) {
            setSuffix(hash(suffix));
        }

        return true;
    }

    private boolean set(int blockId, long fileId) {
        ByteBuffer bf = ByteBuffer.allocate(ENCODE_FILECODE_LENGTH);
        bf.order(ByteOrder.LITTLE_ENDIAN);
        bf.putInt(blockId);
        bf.putLong(fileId);
        this.fileCode = bf.array();
        return true;
    }

    public String get() {
        return get(this.tfsClusterIndex, false, false);
    }

    public String get(char tfsClusterIndex) {
        return get(tfsClusterIndex, false, false);
    }

    public String get(boolean isLarge, boolean simple) {
        return get(tfsClusterIndex, isLarge, simple);
    }

    public String get(char tfsClusterIndex, boolean isLarge, boolean simple) {
        char tfsNameHead = isLarge ? TFS_LARGE_KEY_CHAR : TFS_SMALL_KEY_CHAR;
        byte[] name = new byte[MAIN_FILENAME_LENGTH];
        name[0] = (byte)(tfsNameHead & 0xff);
        name[1] = (byte)(tfsClusterIndex & 0xff);

        byte[] code = encode(this.fileCode, simple);
        System.arraycopy(code, 0, name, 2, ENCODE_FILENAME_LENGTH);
        return new String(name);
    }

    public char getTfsClusterIndex() {
        return tfsClusterIndex;
    }

    public void setTfsClusterIndex(char tfsClusterIndex) {
        this.tfsClusterIndex = tfsClusterIndex;
    }


}
