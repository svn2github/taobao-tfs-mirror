package com.taobao.gulu.common.tfs.utility;

public class NameMetaDirHelper {
	private String dirName;
    private boolean canBeDeleted;

    public NameMetaDirHelper(String dirName) 
    {
        this.dirName = dirName;
        this.canBeDeleted = false;
    }

    public boolean canBeDeleted() 
    {
        return canBeDeleted;        
    }

    public void setCanBeDeleted(boolean flag) 
    {
        canBeDeleted = flag;
    }

    public String getDirName() 
    {
        return dirName;
    }
}
