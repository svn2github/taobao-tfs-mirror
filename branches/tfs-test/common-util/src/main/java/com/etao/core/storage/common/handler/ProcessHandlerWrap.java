package com.etao.core.storage.common.handler;

import com.etao.gaia.handler.ProcessHandler;
import com.etao.gaia.handler.staf.ProcessHandlerSTAFImpl;

public class ProcessHandlerWrap {
	private static ProcessHandlerWrap handlerWrap = new ProcessHandlerWrap();
	private static ProcessHandler processHandler = ProcessHandlerSTAFImpl.getProcessHandler();
	
	private ProcessHandlerWrap(){}
	
	public static ProcessHandlerWrap getProcessHandlerWrap(){
		return handlerWrap;
	}
	
	public static ProcessHandler getProcessHandler(){
		return processHandler;
	}
	
}
