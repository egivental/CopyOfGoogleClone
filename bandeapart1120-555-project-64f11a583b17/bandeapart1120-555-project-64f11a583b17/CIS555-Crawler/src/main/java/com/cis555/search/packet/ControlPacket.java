package com.cis555.search.packet;

import java.io.Serializable;

public class ControlPacket implements Serializable {
	
	public static transient int Reporting = 0;
	
	public final int signal;
	
	public ControlPacket(int signal) {
		this.signal = signal;
	}

}