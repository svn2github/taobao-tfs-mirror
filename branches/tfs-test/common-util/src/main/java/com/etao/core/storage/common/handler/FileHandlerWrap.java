package com.etao.core.storage.common.handler;

import com.etao.gaia.handler.FileHandler;
import com.etao.gaia.handler.staf.FileHandlerSTAFImpl;

public class FileHandlerWrap {
	private static FileHandlerWrap handlerWrap = new FileHandlerWrap();
	private static FileHandler fileHandler = FileHandlerSTAFImpl.getFileHandler();
	
	private FileHandlerWrap(){}
	
	public static FileHandlerWrap getFileHandlerWrap(){
		return handlerWrap;
	}
	
	public static FileHandler getFileHandler(){
		return fileHandler;
	}
}
