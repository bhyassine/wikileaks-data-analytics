package app;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.PageLoader;

class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text refid, Iterable<Text> occurences, Context context)
			throws IOException, InterruptedException {

		if (refid.toString().equals(Crawler.errorToken)) {
			// We have to report the errors
			StringBuilder errorMsg = new StringBuilder();
			for (Text e : occurences) {
				errorMsg.append(Crawler.newline);
				errorMsg.append(Crawler.errorToken);
				errorMsg.append(Crawler.separator);
				errorMsg.append(e.toString());
			}
			context.write(new Text(String.valueOf(Crawler.newline)), new Text(
					errorMsg.toString()));
		} else {
			// Download and save the page
			FileSystem fs = FileSystem.get(context.getConfiguration());

			String fileName = refid.toString();
			String outputFolder = Crawler.pagesSavingLocation;
			String extension = Crawler.gzCompressionInOutput ? "gz" : "html";
			String outputPath = outputFolder + "/" + fileName + "." + extension;
			String urlStr = Crawler.downloaderURL + "/" + fileName + ".html";

			if (PageLoader.getAndSave(fs, outputPath, urlStr,
					Crawler.gzCompressionInOutput)) {
				context.write(new Text("SAVED"), refid);
			} else {
				context.write(
						new Text(Crawler.errorToken),
						new Text(String.format("Cannot get&save page %s",
								outputPath)));
			}
		}
	}
}