package com.taobao.gulu.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * @author diqing
 * 
 */

public class Uniquetair implements Serializable{
private static final long serialVersionUID = 5922972048163923874L;
    
    private String tfsName;
    private int referenceCount;
    private int version;

    public String getTfsName() {
        return tfsName;
    }
    public void setTfsName(String tfsName) {
        this.tfsName = tfsName;
    }
    public int getReferenceCount() {
        return referenceCount;
    }
    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }
    public int getVersion() {
        return version;
    }
    public void setVersion(int version) {
        this.version = version;
    }
    public Uniquetair(String tfsName, int referenceCount) {
        this(tfsName, referenceCount, 0);
    }
    public Uniquetair(String tfsName, int referenceCount, int version) {
        super();
        this.tfsName = tfsName;
        this.referenceCount = referenceCount;
        this.version = version;
    }
    public Uniquetair() {
        this.tfsName = "";
        this.referenceCount = 0;
        this.version = 0;
    }
    
    public void subRef(int diff) {
        this.referenceCount -= diff;
    }
    
    public void addRef(int diff) {
        this.referenceCount += diff;
    }
}
