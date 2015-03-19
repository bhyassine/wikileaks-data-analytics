package hadoop;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import parsing.HTMLParser;

public class HTMLParsingMapReduce {
	private static String jsoupSelect = "";

	/**
	 * Custom Mapper for HTML parsing task.
	 * 
	 * @author Florian Briant
	 *
	 */
	public static class HTMLParsingMapReduceMapper extends
			Mapper<Text, Text, Text, Text> {
		/**
		 * Custom map. Get the name of the HTML file, parses it, and writes the
		 * pair (pageID, text results)
		 * 
		 * @param key
		 *            the output file of the task (I
		 * @param value
		 *            the name of HTML the file to process (pageID)
		 * @param context
		 *            the job context
		 */
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String res = HTMLParser.parseHDFS(key.toString(), jsoupSelect,
					FileSystem.get(conf));
			context.write(key, new Text(res));
		}

	}

	/**
	 * Dummy Reducer. Rewrites the pair it received.
	 * 
	 * @author Florian Briant
	 *
	 */
	public static class HTMLParsingMapReduceReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}

	/**
	 * Set the map-reduce job. Set custom InputFormat, RecordReader, Mapper and
	 * Reducer such that each HTML file is processed in one map.
	 * 
	 * @param input
	 *            the input HDFS directory
	 * @param output
	 *            the output HDFS directory
	 * @param jsp
	 *            the JSOUP selection criteria
	 * @throws Exception
	 */
	public static void launchMapReduce(String input, String output, String jsp)
			throws Exception {
		jsoupSelect = jsp;
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WikiLeaks - HTML Parsing");
		job.setJarByClass(HTMLParsingMapReduce.class);
		job.setMapperClass(HTMLParsingMapReduceMapper.class);
		job.setInputFormatClass(HTMLParsingInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setCombinerClass(HTMLParsingMapReduceReducer.class);
		job.setReducerClass(HTMLParsingMapReduceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(50);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}