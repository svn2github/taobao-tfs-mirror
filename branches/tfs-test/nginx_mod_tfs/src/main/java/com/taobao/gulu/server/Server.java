package com.taobao.gulu.server;

/**
 * @author gongyuan.cz
 *
 */

public interface Server {
	
	/**
	 * start the Server by default configure file
	 * @return true: start successfully    false: fail
	 */
	public boolean start();
	
	/**
	 * stop the Server by default configure file
	 * @return true: stop successfully    false: fail
	 */
	public boolean stop();
	
	/**
	 * start the Server by allocate configure file
	 * @return true: start successfully    false: fail
	 */
	public boolean start(String configFileName);
	
	/**
	 * stop the Server by allocate configure file
	 * @return true: stop successfully    false: fail
	 */
	public boolean stop(String configFileName) ;
	
	/**
	 * restart the Server by default configure file
	 * @return true: restart successfully    false: fail
	 */
	public boolean restart();
	
	/**
	 * restart the Server by allocate configure file
	 * @return true: restart successfully    false: fail
	 */
	public boolean restart(String configFileName);
	
	/**
	 * control the Server by allocate configure file
	 * 
	 * @throws Exception 
	 */
	public void doServerCtl(String conf, String action) throws Exception;
	
	/**
	 * control the Server by allocate configure file
	 * check the process message
	 * @throws Exception 
	 */
	public void doServerCtl(String conf, String action, String expectInputMessage,
			String expectErrorMessage) throws Exception;
	
	/**
	 * detect the Server status
	 * @return true: alive    false: stop
	 */
	public boolean detectServerStatus();
	
	/**
	 * start the Server fail and varify the errorMessage
	 */
	public void startServerError(String configFileName, String errorMessage);
	
	
	/**
	 * stop the Server fail and varify the errorMessage
	 */
	public void stopServerError(String configFileName, String errorMessage);

}
