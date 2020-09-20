package com.maxmind.db;

import java.net.InetAddress;
import java.util.Map;

/** Low-alloc database query interface.
 * The way this works is that you provide a specification of
 * "areas of interest", providing callbacks.
 */
public class AreasOfInterest { // TODO: Rename to 'CallbackAPI'?

    public static abstract class Callback<X> {}

    public static abstract class TextNode<X> extends Callback<X>{
	public abstract void apply(X state, String value);
    }

    public static abstract class ObjectNode<X> {
	private final Map<String, Callback<X>> fieldsOfInterest;

	public ObjectNode(Map<String, Callback<X>> fieldsOfInterest) {
	    this.fieldsOfInterest = fieldsOfInterest;
	}

	public void objectBegin(X state) {}
	public void objectEnd(X state) {}
    }

    public static abstract class RecordCallback<X> extends ObjectNode<X> {
	public RecordCallback(Map<String, Callback<X>> fieldsOfInterest) {
	    super(fieldsOfInterest);
	}
	
	public void network(X state, byte[] ipAddress, int prefixLength) {}
    }

    
}
