package com.maxmind.db;

import java.net.InetAddress;
import java.util.Map;
import java.util.HashMap;

/** Low-alloc database query interface.
 * The way this works is that you provide a specification of
 * "areas of interest", providing callbacks.
 */
public class AreasOfInterest { // TODO: Rename to 'CallbackAPI'?

    public static abstract class Callback<X> {}

    public static abstract class TextNode<X> extends Callback<X> {
	public abstract void setValue(X state, CharSequence value);
    }

    public static class ObjectNode<X> extends Callback<X> {
	private final Map<CharSequence, Callback<X>> fieldsOfInterest;

	public ObjectNode(Map<String, Callback<X>> fieldsOfInterest) {
	    Map<CharSequence, Callback<X>> mapToUse = new HashMap<>();
	    mapToUse.putAll(fieldsOfInterest);
	    this.fieldsOfInterest = mapToUse;
	}

	public Callback<X> callbackForField(CharSequence field) {
	    return fieldsOfInterest.get(field);
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
