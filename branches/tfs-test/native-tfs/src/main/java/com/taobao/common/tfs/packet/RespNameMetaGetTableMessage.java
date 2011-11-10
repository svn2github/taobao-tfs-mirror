/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.common.tfs.packet;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.taobao.common.tfs.common.StreamTranscoderUtil;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.namemeta.MetaTable;

public class RespNameMetaGetTableMessage extends BasePacket {
	private long version;
	private long length;
	private byte[] content;

    public RespNameMetaGetTableMessage(Transcoder transcoder) {
        super(transcoder);
        this.pcode = TfsConstant.RESP_NAME_META_GET_TABLE_MESSAGE;
    }
    
    @Override
    public boolean decode() {
    	version = byteBuffer.getLong();
    	length = byteBuffer.getLong();
    	System.out.println("before decompress: table version: " + version + ", length: " + length);
    	//XXX: long -> int
        if (length > 0)
            content = StreamTranscoderUtil.getByteArray(byteBuffer, (int)length);
        return true;
    }
    
    public boolean getTable(MetaTable metaTable) throws DataFormatException {
	    long tableLen = TfsConstant.MAX_BUCKET_DATA_LENGTH;
	    byte[] table = new byte[(int)tableLen];
	    
    	// decompress table
	    Inflater decompresser = new Inflater();
	    decompresser.setInput(content, 0, (int)length);
	    tableLen = decompresser.inflate(table);
	    decompresser.end();
	    if (tableLen % TfsConstant.LONG_SIZE != 0) {
	    	System.out.println("invalid table len: " + tableLen);
	    	return false;
	    }
    	metaTable.clear();
    	// update table
    	int offset = 0;
    	while (tableLen >= TfsConstant.LONG_SIZE) {
    		long tmp = (long)table[offset + 7] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 6] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 5] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 4] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 3] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 2] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset + 1] & 0xff;
    		tmp <<= 8;
    		tmp |= (long)table[offset] & 0xff;
    		offset += TfsConstant.LONG_SIZE;
    		tableLen -= TfsConstant.LONG_SIZE;
    		metaTable.push(tmp);
    	}
    	metaTable.setVersion(version);
		
		return true;
    }
}