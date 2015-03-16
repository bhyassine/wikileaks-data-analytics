package app;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text refid, Iterable<Text> dontCare, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		// TODO: do something with the html file corresponding to its
		// pair.page_id
		// TODO: handle case with refid = errorToken
		context.write(refid, new Text("downloaded"));
	}
}