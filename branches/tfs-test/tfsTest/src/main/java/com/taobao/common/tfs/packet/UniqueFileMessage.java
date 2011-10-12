/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import com.taobao.common.tfs.common.TfsConstant;

public class UniqueFileMessage extends BasePacket {

    private int command;
    private int crc;
    private int size;
    private int referenceCount;
    private int optionFlag;
    String tfsName;

    public static final int UNIQUE_FILE_RESPONSE = 1;
    public static final int UNIQUE_FILE_QUERY = 2;
    public static final int UNIQUE_FILE_PLUS =100;
    public static final int UNIQUE_FILE_MINUS = 101;
    public static final int UNIQUE_FILE_RENAME = 102;
    public static final int UNIQUE_FILE_INSERT = 103;
    public static final int UNIQUE_FILE_DELETE = 104;

    public static final int MAX_FILE_NAME_LENGTH = 64;

    public UniqueFileMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.UNIQUE_FILE_MESSAGE;
    }

    @Override
    public boolean decode() {
        command = byteBuffer.getInt();
        crc = byteBuffer.getInt();
        size = byteBuffer.getInt();
        referenceCount = byteBuffer.getInt();
        byte[] name = new byte[MAX_FILE_NAME_LENGTH];
        byteBuffer.get(name, 0, MAX_FILE_NAME_LENGTH);
        // trim right 0
        int length = 0;
        for (byte c : name) {
            if (c != 0) {
                ++length;
            } else {
                break;
            }
        }
        tfsName = new String(name, 0, length);
        optionFlag = byteBuffer.getInt();
        return true;
    }

    @Override
    public int getPacketLength() {
        return TfsConstant.INT_SIZE  * 5 + MAX_FILE_NAME_LENGTH;
    }

    @Override
    public void writePacketStream() {
        byteBuffer.putInt(command);
        byteBuffer.putInt(crc);
        byteBuffer.putInt(size);
        byteBuffer.putInt(referenceCount);
        byteBuffer.put(tfsName.getBytes());

        byteBuffer.position(byteBuffer.position() + MAX_FILE_NAME_LENGTH - tfsName.length());
        byteBuffer.putInt(optionFlag);
    }

    public int getCommand() {
        return command;
    }

    public void setCommand(int command) {
        this.command = command;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public int getOptionFlag() {
        return optionFlag;
    }

    public void setOptionFlag(int optionFlag) {
        this.optionFlag = optionFlag;
    }

    public String getTfsName() {
        return tfsName;
    }

    public void setTfsName(String tfsName) {
        this.tfsName = tfsName;
    }

}
