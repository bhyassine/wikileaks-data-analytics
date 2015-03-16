package app;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Provide basic interface for building a URL
 * 
 * inspired by: http://stackoverflow.com/a/1861774
 */
public class QueryString {

	private String query = "";

	public QueryString(HashMap<String, String> map)
			throws UnsupportedEncodingException {
		Iterator<Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> pair = it.next();
			query += URLEncoder.encode(pair.getKey(), "utf-8") + "="
					+ URLEncoder.encode(pair.getValue(), "utf-8");
			if (it.hasNext()) {
				query += "&";
			}
		}
	}

	public QueryString(String name, String value)
			throws UnsupportedEncodingException {
		query = URLEncoder.encode(name, "utf-8") + "="
				+ URLEncoder.encode(value, "utf-8");
	}

	public QueryString() {
		query = "";
	}

	public void add(String name, String value)
			throws UnsupportedEncodingException {
		if (!query.trim().equals(""))
			query += "&";
		query += URLEncoder.encode(name, "utf-8") + "="
				+ URLEncoder.encode(value, "utf-8");
	}

	public String toString() {
		return query;
	}
}
