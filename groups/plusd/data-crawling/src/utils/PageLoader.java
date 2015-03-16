package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

	public static boolean getAndSave(FileSystem fs, String dst, String urlStr,
			boolean gzCompression) {
		boolean sucess = false;

		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection) (new URL(urlStr)).openConnection();
			InputStream pageIS = connection.getInputStream();
			OutputStream outputOS = fs.create(new Path(dst));

			// Save the page
			if (gzCompression) {
				GZIPOutputStream outputGIS = new GZIPOutputStream(outputOS);
				org.apache.hadoop.io.IOUtils.copyBytes(pageIS, outputGIS,
						fs.getConf());
				outputGIS.close();
			} else {
				org.apache.hadoop.io.IOUtils.copyBytes(pageIS, outputOS,
						fs.getConf());
			}

			outputOS.close();
			pageIS.close();
			sucess = true;
		} catch (IOException e) {
			sucess = false;
		}

		return sucess;
	}

}
