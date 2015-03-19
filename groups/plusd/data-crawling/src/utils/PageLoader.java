package utils;

import java.io.IOException;
import java.io.InputStream;
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
		HttpURLConnection connection;
		connection = (HttpURLConnection) (new URL(url)).openConnection();
		InputStream pageIS = connection.getInputStream();
		String content = IOUtils.toString(pageIS);
		pageIS.close();
		return content;
	}

	public static void getAndSave(FileSystem fs, String dst, String urlStr,
			boolean gzCompression) throws IOException {

		HttpURLConnection connection;
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
	}

}
