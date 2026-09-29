package com.example.types;

import java.io.*;

public class Payload implements Externalizable {
	private static final long serialVersionUID = -113741830671144814L;

	public int cid;
	public int xnr;
	public byte[] any;
	

	public Payload() {}
	public Payload(int cid, int xnr, byte[] any) {
		this.cid = cid;
		this.xnr = xnr;
		this.any = any;
	}
	
	public int getCommandId() { return this.xnr; }
	public void setCommandId(int xnr) { this.xnr = xnr; }
	
	public int getClientId() { return this.cid; }
	public void setClientId(int cid) { this.cid = cid; }
	
	public byte[] getAny() { return this.any; }
	public void setAny(byte[] any) { this.any = any; }
	
	/*******************
	 ** SERIALIZATION **
	 *******************/
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(cid);
		out.writeInt(xnr);

		if (any == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(any.length);
			out.write(any);
		}
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.cid = in.readInt();
		this.xnr = in.readInt();

		int len = in.readInt();
		if (len >= 0) {
			this.any = new byte[len];
			in.readFully(any);
		}
	}
	
	public static void writeDataStream(DataOutputStream dos, Payload p) throws IOException {
		dos.writeInt(p.cid);
		dos.writeInt(p.xnr);
		
		if (p.any == null) {
			dos.writeInt(-1);
		} else {
			dos.writeInt(p.any.length);
			dos.write(p.any);
		}
	}
	
	public static Payload readDataStream(DataInputStream dis) throws IOException {
		int cid = dis.readInt();
		int xnr = dis.readInt();
		
		int size = dis.readInt();
		byte[] any = null;
		if (size >= 0) {
			any = new byte[size];
			dis.readFully(any);
		}
		return new Payload(cid, xnr, any);
	}

	@Override
	public String toString() {
		return "(Command/Result for [" + cid + "|" + xnr + "]: " + (any == null ? "null" : any.length) + ")";
	}
}
