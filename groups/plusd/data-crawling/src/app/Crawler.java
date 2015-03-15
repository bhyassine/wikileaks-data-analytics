package app;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

	// Minimum/Maximum plusD available date
	// https://www.wikileaks.org/plusd/
	public static String defaultFrom = "1966-01-01";
	public static String defaultTo = "2010-12-31";

	public static enum Errors {
		NONE(0), BAD_NB_ARGS(1), INTERNAL_UNEXPECTED_CASE(2), JOB_COMPLETION(3), INPUT_FILE_WRITE(
				4), UNASSIGNED_ERROR_CODE(5);

		private int value;

		private Errors(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	};

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

	public static boolean writeInputFile(FileSystem fs, Path path,
			Date fromDate, Date toDate) throws IOException {
		OutputStreamWriter osw = new OutputStreamWriter(fs.create(path));

		StringBuilder dates = new StringBuilder();

		// TODO: fill dates from the (from, to) dates

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
		int returnCode = Errors.UNASSIGNED_ERROR_CODE.getValue();

		// Getting the input (from, to) dates
		SimpleDateFormat defaultDF = new SimpleDateFormat("yyyy-MM-dd");
		Date fromDate = defaultDF.parse(defaultFrom);
		Date toDate = defaultDF.parse(defaultTo);
		if (args.length != 2) {
			System.out
					.println("N.B. Possible arguments are: <from-year> <to-year>");
			System.out.println(String.format("-> Using defaults: %s to %s",
					defaultFrom, defaultTo));
		} else {
			String fromInputStr = args[0];
			String toInputStr = args[1];
			try {
				fromDate = defaultDF.parse(fromInputStr);
				toDate = defaultDF.parse(toInputStr);
			} catch (ParseException pe) {
				System.out.println(String.format(
						"Failed to parse input dates %s and %s", fromInputStr,
						toInputStr));
				System.out.println(String.format("-> Using defaults: %s to %s",
						defaultFrom, defaultTo));
			}
		}

		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wikileaks - PlusD data Crawler");
		FileSystem fs = FileSystem.get(conf);

		job.setJarByClass(Crawler.class);
		job.setMapperClass(CrawlerMapper.class);
		job.setReducerClass(CrawlerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		TextInputFormat.setMaxInputSplitSize(job, Long.MAX_VALUE);

		// TODO: extract from-to dates as arguments

		// Create & add to program dateInputFile
		if (writeInputFile(fs, datesInputPath, fromDate, toDate)) {
			TextInputFormat.addInputPath(job, datesInputPath);

			// Run the job
			if (job.waitForCompletion(true)) {
				returnCode = Errors.NONE.getValue();
			} else {
				returnCode = Errors.JOB_COMPLETION.getValue();
			}

			// Do the cleaning
			fs.delete(datesInputPath, true);
		} else {
			returnCode = Errors.INPUT_FILE_WRITE.getValue();
		}

		return returnCode;
	}
}
