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
		int returnCode = Errors.UNASSIGNED_ERROR_CODE.getValue();

		if (args.length != 2) {
			System.out.println("Arguments: <from-year> <to-year>");
			System.out.println("Example: 1973 2010");

			returnCode = Errors.BAD_NB_ARGS.getValue();
		} else {
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
			if (writeInputFile(fs, datesInputPath)) {
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
		}

		return returnCode;
	}
}
