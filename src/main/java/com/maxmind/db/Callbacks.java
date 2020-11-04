package com.maxmind.db;

import java.net.InetAddress;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;

/** Callbacks for the low-allocation database query interface.
 * This lets you build a specification of which object paths you want callback for.
 */
public class Callbacks {

    public static interface Callback<X> {}

    @FunctionalInterface
    public static interface TextNode<X> extends Callback<X> {
	public abstract void setValue(X state, CharSequence value);

	// Utility function:
	public static void assignToStringBuilder(StringBuilder sb, CharSequence value) {
	    sb.setLength(0);
	    sb.append(value);
	}
    }

    @FunctionalInterface
    public static interface DoubleNode<X> extends Callback<X> {
	public abstract void setValue(X state, double value);
    }

    public static class ObjectNode<X> implements Callback<X> {
	private final Map<CharSequence, Callback<X>> fieldsOfInterest;

	public ObjectNode(Map<String, Callback<X>> fieldsOfInterest) {
	    Map<CharSequence, Callback<X>> mapToUse = new TreeMap<>(CHARSEQ_COMPARATOR);
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


    private static class CharSequenceComparator implements Comparator<CharSequence> {
	public int compare(CharSequence a, CharSequence b) {
	    int lenA = a.length(), lenB = b.length();
	    int minLen = Math.min(lenA, lenB);

	    int i = 0;
	    while (i < minLen) {
		char cA = a.charAt(i);
		char cB = b.charAt(i);
		if (cA != cB) {
		    return cA<cB ? -1 : 1;
		}
		i++;
	    }

	    if (lenA != lenB) {
		return lenA < lenB ? -1 : 1;
	    }
	    return 0;
	}
    }

    private static final Comparator<CharSequence> CHARSEQ_COMPARATOR = new CharSequenceComparator();

    public static class ObjectCallbackBuilder<X> {
	private final Map<String, Callback<X>> here = new HashMap<>();
	private final Map<String, ObjectCallbackBuilder<X>> deeper = new HashMap<>();

	public ObjectCallbackBuilder() {}

	public void text(String key, TextNode<X> callback) {
	    if (deeper.containsKey(key)) throw new IllegalStateException("A record is already registered here: '"+key+"'");
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    here.put(key, callback);
	}

	public void number(String key, DoubleNode<X> callback) {
	    if (deeper.containsKey(key)) throw new IllegalStateException("A record is already registered here: '"+key+"'");
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    here.put(key, callback);
	}

	private ObjectCallbackBuilder<X> getOrCreateSubObject(String key) {
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    ObjectCallbackBuilder<X> r = deeper.get(key);
	    if (r == null) {
		deeper.put(key, r = new ObjectCallbackBuilder<X>());
	    }
	    return r;
	}

	public ObjectCallbackBuilder<X> obj(String key) {
	    ObjectCallbackBuilder<X> subObject = getOrCreateSubObject(key);
	    return subObject;
	}

	public ObjectNode<X> build() {
	    return new ObjectNode<X>(buildMap());
	}

	protected Map<String, Callback<X>> buildMap() {
	    Map<String, Callback<X>> fieldsOfInterest = new HashMap<>(here);
	    for (Map.Entry<String, ObjectCallbackBuilder<X>> e : deeper.entrySet()) {
		fieldsOfInterest.put(e.getKey(), e.getValue().build());
	    }
	    return fieldsOfInterest;
	}
    }

    public static class RecordCallbackBuilder<X> extends ObjectCallbackBuilder<X> {
	public RecordCallbackBuilder() {}

	@Override
    	public RecordCallback<X> build() {
	    return new RecordCallback<X>(buildMap()) {};
	}
    }

}
