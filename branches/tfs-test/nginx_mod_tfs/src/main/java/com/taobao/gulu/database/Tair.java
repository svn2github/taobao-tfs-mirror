package com.taobao.gulu.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.unique.UniqueValue;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * @author gongyuan.cz
 * 
 */

public class Tair {
	private DefaultTairManager tairManager = new DefaultTairManager();
	private int area = 0;
	private String master_address = "0.0.0.0:0000";
	private String slave_address = "0.0.0.0:0000";
	private String group_name = "group_1";
	private int time_out = 2000;
	
	public int getArea() {
		return area;
	}
	public void setArea(int area) {
		this.area = area;
	}
	public String getMaster_address() {
		return master_address;
	}
	public void setMaster_address(String master_address) {
		this.master_address = master_address;
	}
	public String getSlave_address() {
		return slave_address;
	}
	public void setSlave_address(String slave_address) {
		this.slave_address = slave_address;
	}
	public String getGroup_name() {
		return group_name;
	}
	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}
	public int getTime_out() {
		return time_out;
	}
	public void setTime_out(int time_out) {
		this.time_out = time_out;
	}
	
	public void init(){
		// init tair
		List<String> cs = new ArrayList<String>();
		cs.add(master_address);
		cs.add(slave_address);
		tairManager.setConfigServerList(cs);
		tairManager.setGroupName(group_name);
		tairManager.setTimeout(time_out);
		tairManager.init();
	}
	
	public void put(String key, String value) {
		ResultCode rs = tairManager.put(area, key, value);
		Assert.assertTrue("put data fail!", rs.isSuccess());
	}
	
	public void delete(String key) {
		ResultCode rs = tairManager.delete(area, key);
		Assert.assertTrue("delete data fail!", rs.isSuccess());
	}
	
	public byte[] get(byte[] key) {
		Result<DataEntry> rs = tairManager.get(area, key);
		Assert.assertTrue("get data fail!", rs.isSuccess());
		//System.out.println(rs.getRc());
		if (rs.getValue() == null) {
			return null;
		}
		//return   (byte[]) rs.getValue().getValue();
		return (byte[]) rs.getValue().getValue();
//		System.out.println(a.length);
//		System.out.println(new String (a));
		
//		if(rs.getValue().getValue() instanceof java.lang.String){
//		System.out.println("ffffff");
//		} 
//		if(rs.getValue().getValue() instanceof java.lang.reflect.Array){
//			System.out.println("aaaa");
//			}
//		else
//			System.out.println(rs.getValue().getValue());
//			System.out.println(rs.getRc());

	}
	
	//根据上传的文件计算该文件在tair中的key
	public byte[] getKey( byte[] data, int offset, int length) 
	{
        try 
        {
            MessageDigest algo = MessageDigest.getInstance("MD5");
            algo.update(data, offset, length);
            ByteBuffer bf = ByteBuffer.allocate(4 + algo.getDigestLength()); // algo.getDigestLength() == 16
            bf.put(algo.digest());
            bf.putInt(algo.getDigestLength(), length);

            return bf.array();
        } 
        
        catch (NoSuchAlgorithmException e)
        {
        }
        return null;
    }
	// 根据文件名转成byte数组
	public byte[]  getByte(String fileName) throws IOException
	{
		InputStream in = new FileInputStream(fileName);
	    byte[] data= new byte[in.available()];
	    in.read(data);
	    return data;
	}
	
	public UniqueValue query(byte[] key) throws TfsException 
	{
        UniqueValue value = null;
        DataEntry data = null;

        Result<DataEntry> ret = tairManager.get(this.area, key);
        if (ret.getRc().isSuccess()) 
        {
            if (ret.getRc() == ResultCode.DATANOTEXSITS) 
            {
                value = new UniqueValue("", TfsConstant.UNIQUE_QUERY_NOT_EXIST);
            } 
            else if ((data = ret.getValue()) != null)
            {
                value =  decode((byte[])data.getValue());
                value.setVersion(data.getVersion());
            }
        } 
        else 
        {
            throw new TfsException("TAIR query failed" + ret.getRc().getMessage());
        }
        return value;
    }

    public UniqueValue decode(byte[] data) {
        ByteBuffer bf = ByteBuffer.wrap(data);
        int tfsNameLen = data.length - 4;
        byte[] tfsName = new byte[tfsNameLen];

        UniqueValue value = new UniqueValue();
        value.setReferenceCount(bf.getInt());
        bf.get(tfsName, 0, tfsNameLen);
        value.setTfsName(new String(tfsName));
        return value;
    }
	
}
