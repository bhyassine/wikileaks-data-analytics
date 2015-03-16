package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.io.IOUtils;

public class PageLoader {

	public static String get(String url) throws MalformedURLException,
			IOException {
		HttpURLConnection connection = (HttpURLConnection) (new URL(url))
				.openConnection();
		BufferedReader qBF = new BufferedReader(new InputStreamReader(
				connection.getInputStream()));
		String content = IOUtils.toString(qBF);
		qBF.close();

		return content;
	}

}
