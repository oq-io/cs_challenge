package com.oq_io.cs_challenge.dao;

import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.google.gson.Gson;

@Entity
@Table(name = "event")
public class Event {
	
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private int id_pk;
	
	@Column(name = "id")
	private String id;
	
	public enum State {
		STARTED, 
		FINISHED
	}
	
	@Enumerated(EnumType.STRING)
	@Column(name = "state")
	private State state;
	
	@Column(name = "type")
	private String type;
	
	@Column(name = "host")
	private String host;
	
	@Column(name = "timestamp")
	private Long timestamp;
	  
	private Long duration;
	private boolean alert;
	
	public Event() {
		
	}
	
	public Event(String id, String state, String type, String host, long timestamp) {
		this.id = id;
		this.state = State.valueOf(state);
		this.type = type;
		this.host = host;
		setTimestamp(timestamp);
	}
	
	public String getLog() {
		return new Gson().toJson(this);
	}
	
	public String getId() {
		return id;
	}
	
	public String getType() {
		return type;
	}
	
	public State getState() {
		return state;
	}
	
	private void setTimestamp(long timestamp) {
		try {
			this.timestamp = new Timestamp(timestamp).getTime();
		}catch(Exception e) { 
			
		}
	}

	public void setDuration(Long duration) {
		this.duration = duration;	
	}
	
	public void setAlert(boolean alert) {
		this.alert = alert;	
	}
	

}

