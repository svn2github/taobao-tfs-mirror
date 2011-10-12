package com.taobao.common.tfs.unique;

import java.io.Serializable;

public class UniqueValue implements Serializable {
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
    public UniqueValue(String tfsName, int referenceCount) {
        this(tfsName, referenceCount, 0);
    }
    public UniqueValue(String tfsName, int referenceCount, int version) {
        super();
        this.tfsName = tfsName;
        this.referenceCount = referenceCount;
        this.version = version;
    }
    public UniqueValue() {
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
