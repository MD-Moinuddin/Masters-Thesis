package com.example.types;

import java.io.Serializable;

public interface Application extends Serializable {
	
	public void init();
	public byte[] execute(Payload op);	
	public byte[] getState();
	public void applyState(byte[] state);
}
