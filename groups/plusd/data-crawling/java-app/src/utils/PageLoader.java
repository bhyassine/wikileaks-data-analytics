package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

public class PageLoader {

	// Inspired from
	// http://www.mkyong.com/java/java-httpurlconnection-follow-redirect-example/
	public static InputStream getInputStream(String url)
			throws MalformedURLException, IOException {
		URL obj = new URL(url);
		HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
		conn.setReadTimeout(5000);
		conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
		conn.addRequestProperty("User-Agent", "Mozilla");
		conn.addRequestProperty("Referer", "google.com");

		boolean redirect = false;

		// normally, 3xx is redirect
		int status = conn.getResponseCode();
		if (status != HttpURLConnection.HTTP_OK) {
			if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM
					|| status == HttpURLConnection.HTTP_SEE_OTHER)
				redirect = true;
		}

		if (redirect) {

			// get redirect url from "location" header field
			String newUrl = conn.getHeaderField("Location");

			// get the cookie if need, for login
			String cookies = conn.getHeaderField("Set-Cookie");

			// open the new connnection again
			conn = (HttpURLConnection) new URL(newUrl).openConnection();
			conn.setRequestProperty("Cookie", cookies);
			conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
			conn.addRequestProperty("User-Agent", "Mozilla");
			conn.addRequestProperty("Referer", "google.com");

		}

		return conn.getInputStream();
	}

	public static String get(String url) throws MalformedURLException,
			IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(
				PageLoader.getInputStream(url)));
		String inputLine;
		StringBuffer html = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			html.append(inputLine);
		}
		in.close();
		return html.toString();
	}

	public static void getAndSave(File outputFile, String urlStr,
			boolean gzCompression) throws IOException {

		InputStream pageIS = PageLoader.getInputStream(urlStr);
		if (!outputFile.exists()) {
			outputFile.createNewFile();
		}
		FileOutputStream outputFileOS = new FileOutputStream(outputFile, false);

		// Save the page
		if (gzCompression) {
			GZIPOutputStream outputGIS = new GZIPOutputStream(outputFileOS);
			IOUtils.copy(pageIS, outputGIS);
			outputGIS.close();
		} else {
			IOUtils.copy(pageIS, outputFileOS);
		}

		outputFileOS.close();
		pageIS.close();
	}
}
