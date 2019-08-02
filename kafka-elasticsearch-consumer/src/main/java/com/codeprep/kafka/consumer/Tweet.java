package com.codeprep.kafka.consumer;

public class Tweet {

	private String username;
	private String msg;
	private String timestamp;
	
	public Tweet(String username, String msg, String timestamp) {
		super();
		this.username = username;
		this.msg = msg;
		this.timestamp = timestamp;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

}
