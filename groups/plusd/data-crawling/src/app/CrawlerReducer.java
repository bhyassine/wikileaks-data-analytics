package app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text refid, Iterable<Text> occurences, Context context)
			throws IOException, InterruptedException {

		if (refid.toString().equals(Crawler.errorToken)) {
			// We have to report the errors
			StringBuilder errorMsg = new StringBuilder();
			for (Text e : occurences) {
				errorMsg.append(Crawler.newline);
				errorMsg.append("ERROR: ");
				errorMsg.append(e.toString());
			}
			context.write(new Text("ERRORS_BELOW"),
					new Text(errorMsg.toString()));
		} else {
			// Download and save the page
			Configuration conf = context.getConfiguration();
			context.write(new Text("SAVED"), refid);

			// TODO: download the page
		}
	}
}