package com.maxmind.db;

import java.net.InetAddress;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

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

}
