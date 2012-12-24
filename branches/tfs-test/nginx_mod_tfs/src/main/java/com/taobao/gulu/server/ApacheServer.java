package com.taobao.gulu.server;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import com.taobao.gulu.help.proc.HelpProc;

/**
 * @author gongyuan.cz
 * 
 */

public class ApacheServer extends HelpProc implements Server {

	private String host = "0.0.0.0";
	private int port = -1;
	private String root_url_adress = "http://" + host + ":" + port + "/";
	private String conf_file_directory = "";
	private String execute_cmd = "";
	private String action_start = "start";
	private String action_stop = "stop";
	private String action_restart = "restart";
	private String default_conf = "";
	private String username = "";
	private String password = "";

	public String getExecute_cmd() {
		return execute_cmd;
	}

	public void setExecute_cmd(String execute_cmd) {
		this.execute_cmd = execute_cmd;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getRoot_url_adress() {
		return root_url_adress;
	}

	public void setRoot_url_adress(String root_url_adress) {
		this.root_url_adress = root_url_adress;
	}

	public String getConf_file_directory() {
		return conf_file_directory;
	}

	public void setConf_file_directory(String conf_file_directory) {
		this.conf_file_directory = conf_file_directory;
	}

	public String getAction_start() {
		return action_start;
	}

	public void setAction_start(String action_start) {
		this.action_start = action_start;
	}

	public String getAction_stop() {
		return action_stop;
	}

	public void setAction_stop(String action_stop) {
		this.action_stop = action_stop;
	}

	public String getAction_restart() {
		return action_restart;
	}

	public void setAction_restart(String action_restart) {
		this.action_restart = action_restart;
	}

	public String getDefault_conf() {
		return default_conf;
	}

	public void setDefault_conf(String default_conf) {
		this.default_conf = default_conf;
	}

	@Override
	public boolean start() {
		return start(default_conf);
	}

	@Override
	public boolean stop() {
		return stop(default_conf);
	}

	@Override
	public boolean start(String configFileName) {
		String conf = conf_file_directory + configFileName;
		try {
			doServerCtl(conf, action_start);
			return detectServerStatus();
		} catch (Exception e) {
			return false;
		}

	}

	@Override
	public boolean stop(String configFileName) {
		String conf = conf_file_directory + configFileName;
		try {
			doServerCtl(conf, action_stop);
			if (detectServerStatus())
				return false;
			else
				return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public boolean restart() {
		String conf = conf_file_directory + default_conf;
		try {
			doServerCtl(conf, action_restart);
			return detectServerStatus();
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public boolean restart(String configFileName) {
		String conf = conf_file_directory + configFileName;
		try {
			doServerCtl(conf, action_restart);
			return detectServerStatus();
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void doServerCtl(String conf, String action) throws Exception {
		ShellServer server = new ShellServer(host, username, password);
		String cmd = execute_cmd + " -f " + conf + " -k " + action;
		executeRemoteCommand(server, cmd, "", "");
	}

	@Override
	public boolean detectServerStatus() {
		try {
			SocketChannel channel = SocketChannel.open();
			channel.socket().connect(new InetSocketAddress(host, port));
			channel.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void doServerCtl(String conf, String action,
			String expectInputMessage, String expectErrorMessage)
			throws Exception {
		ShellServer server = new ShellServer(host, username, password);
		String cmd = execute_cmd + " -f " + conf + " -k " + action;
		executeRemoteCommand(server, cmd, expectInputMessage,
				expectErrorMessage);
	}

	@Override
	public void startServerError(String configFileName, String errorMessage) {
		String conf = conf_file_directory + configFileName;
		try {
			doServerCtl(conf, action_start, "", errorMessage);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stopServerError(String configFileName, String errorMessage) {
		String conf = conf_file_directory + configFileName;
		try {
			doServerCtl(conf, action_stop, "", errorMessage);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
