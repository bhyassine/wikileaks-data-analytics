package utils;

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

	public QueryString(HashMap<String, String> map) {
		Iterator<Entry<String, String>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> pair = it.next();
			try {
				query += URLEncoder.encode(pair.getKey(), "utf-8") + "="
						+ URLEncoder.encode(pair.getValue(), "utf-8");
			} catch (UnsupportedEncodingException e) {
				// Stop the program, we can do nothing to fix this error
				// at the runtime level
				e.printStackTrace();
			}
			if (it.hasNext()) {
				query += "&";
			}
		}
	}

	public QueryString(String name, String value) {
		try {
			query = URLEncoder.encode(name, "utf-8") + "="
					+ URLEncoder.encode(value, "utf-8");
		} catch (UnsupportedEncodingException e) {
			// Stop the program, we can do nothing to fix this error
			// at the runtime level
			e.printStackTrace();
		}
	}

	public QueryString() {
		query = "";
	}

	public void add(String name, String value) {
		if (!query.trim().equals("")) {
			query += "&";
		}
		try {
			query += URLEncoder.encode(name, "utf-8") + "="
					+ URLEncoder.encode(value, "utf-8");
		} catch (UnsupportedEncodingException e) {
			// Stop the program, we can do nothing to fix this error
			// at the runtime level
			e.printStackTrace();
		}
	}

	public String toString() {
		return query;
	}
}
