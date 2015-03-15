package app;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Crawler extends Configured implements Tool {

	public static Path datesInputPath = new Path("input-dates.txt");

	public static class CrawlerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object a, Text b, Context context) throws IOException,
				InterruptedException {

			// TODO: outputs the pairs (..., page_id) for each page_id of
			// documents found in the interval of the date chunk
			context.write(new Text("a"), new Text("b"));
		}
	}

	public static class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text a, Iterable<Text> bList, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			// TODO: save the html file corresponding to its pair.page_id
			context.write(new Text("c"), new Text("d"));
		}
	}

	public static boolean writeInputFile(FileSystem fs, Path path)
			throws IOException {
		OutputStreamWriter osw = new OutputStreamWriter(fs.create(path));

		StringBuilder dates = new StringBuilder();

		// TODO: fill dates with the actual dates

		osw.write(dates.toString());
		osw.flush();
		osw.close();

		return true;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Crawler(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wikileaks - PlusD data Crawler");
		FileSystem fs = FileSystem.get(conf);
		boolean success = false;

		job.setJarByClass(Crawler.class);
		job.setMapperClass(CrawlerMapper.class);
		job.setReducerClass(CrawlerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		TextInputFormat.setMaxInputSplitSize(job, Long.MAX_VALUE);

		// TODO: from-to dates as arguments?

		// Create & add to program dateInputFile
		if (writeInputFile(fs, datesInputPath)) {
			TextInputFormat.addInputPath(job, datesInputPath);

			// Run the job
			success = job.waitForCompletion(true);

			// Do the cleaning
			fs.delete(datesInputPath, true);
		} else {
			// TODO: error msg?
		}

		return (success ? 0 : 1);
	}
}
