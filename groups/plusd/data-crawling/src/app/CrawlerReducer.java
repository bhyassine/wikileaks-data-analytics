package app;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.PageLoader;

class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text refid, Iterable<Text> occurences, Context context)
			throws IOException, InterruptedException {

		// Download and save the page
		FileSystem fs = FileSystem.get(context.getConfiguration());

		String fileName = refid.toString();
		String outputFolder = Crawler.pagesSavingLocation;
		String extension = Crawler.gzCompressionInOutput ? "gz" : "html";
		String outputPath = outputFolder + "/" + fileName + "." + extension;
		String urlStr = Crawler.downloaderURL + "/" + fileName + ".html";

		PageLoader.getAndSave(fs, outputPath, urlStr,
				Crawler.gzCompressionInOutput);
		context.write(new Text("SAVED"), refid);
	}
}