package app;

import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Sphinxer;

class CrawlerMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object lineObj, Text fromToString, Context context)
			throws IOException, InterruptedException {

		String[] fromToSplit = fromToString.toString().split(
				String.valueOf(Crawler.separator));
		int from = Integer.valueOf(fromToSplit[0]);
		int to = Integer.valueOf(fromToSplit[1]);

		Stack<String> refIDs;
		try {
			refIDs = Sphinxer.askForRefIDListing(from, (to - from + 1));
			for (String refID : refIDs) {
				context.write(new Text(refID), new Text("dontCare"));
			}
		} catch (IllegalArgumentException e) {
			// We can't fix this error, stop the program
			e.printStackTrace();
		}
	}
}
