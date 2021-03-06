package app;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.Sphinxer;

public class Crawler extends Configured implements Tool {

	public static Path tasksInputPath = new Path("tasks-input-file.txt");
	public static Path outputPath = new Path("mapred-output");
	public static String pagesSavingLocation = "pages-crawled";
	public static String downloaderURL = "http://search.wikileaks.org/plusd/cables";
	public static Boolean gzCompressionInOutput = true;
	public static char newline = '\n';
	public static char separator = '\t';

	// Errors are valid (key) reducer outputs (yes, they can happen, and we must
	// be informed)
	public static String errorToken = "error";

	// How many ref_ids do we discover at each step?
	public static int nbRefIDsPerFetch = Sphinxer.QLimit.maxLimit;

	public static enum Errors {
		NONE(0), BAD_NB_ARGS(1), INTERNAL_UNEXPECTED_CASE(2), JOB_COMPLETION(3), INPUT_FILE_WRITE(
				4), UNASSIGNED_ERROR_CODE(5), BAD_ARGS(6), CANNOT_CREATE_SAVING_FOLDER(
				7);

		private int value;

		private Errors(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	};

	public static void writeInputFile(FileSystem fs, Path path, int fromNo,
			int toNo, int stepsSize) throws IOException {

		OutputStreamWriter osw = new OutputStreamWriter(fs.create(path));
		StringBuilder workChunks = new StringBuilder();

		for (int tmpFrom = fromNo; tmpFrom <= toNo; tmpFrom += stepsSize) {
			int tmpTo = Math.min(tmpFrom + stepsSize - 1, toNo);
			workChunks.append(String.format("%s%c%s%c", tmpFrom, separator,
					tmpTo, newline));
		}

		osw.write(workChunks.toString());
		osw.flush();
		osw.close();
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Crawler(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		int returnCode = Errors.UNASSIGNED_ERROR_CODE.getValue();

		// Getting the input (from, to) dates
		int fromNo = Sphinxer.minDocumentNo;
		int toNo = Sphinxer.maxDocumentNo;
		if (args.length != 2) {
			System.out
					.println("N.B. Possible arguments are: <from-documentNo> <to-documentNo>");
			System.out.println(String.format("-> Using defaults: %d to %d",
					fromNo, toNo));
		} else {
			try {
				fromNo = Integer.valueOf(args[0]);
				toNo = Integer.valueOf(args[1]);
			} catch (NumberFormatException e) {
				System.out
						.println(String
								.format("One or both of <from-documentNo>(%s) or <to-documentNo>(%s) cannot be parsed as an integer",
										args[0], args[1]));
				returnCode = Errors.BAD_ARGS.getValue();
			}

			if (Sphinxer.isValidDocumentNo(fromNo) != 0) {
				System.out
						.println(String
								.format("Argument <from-documentNo> `%d` is not in range (%d, %d)",
										fromNo, Sphinxer.minDocumentNo,
										Sphinxer.maxDocumentNo));
				returnCode = Errors.BAD_ARGS.getValue();
			} else if (Sphinxer.isValidDocumentNo(toNo) != 0) {
				System.out
						.println(String
								.format("Argument <to-documentNo> `%d` is not in range (%d, %d)",
										toNo, Sphinxer.minDocumentNo,
										Sphinxer.maxDocumentNo));
				returnCode = Errors.BAD_ARGS.getValue();
			}
		}

		if (returnCode == Errors.UNASSIGNED_ERROR_CODE.getValue()) {
			Configuration conf = this.getConf();
			Job job = Job.getInstance(conf, "Wikileaks - PlusD data Crawler");
			FileSystem fs = FileSystem.get(conf);

			if (fs.mkdirs(new Path(pagesSavingLocation))) {
				job.setJarByClass(Crawler.class);
				job.setMapperClass(CrawlerMapper.class);
				job.setReducerClass(CrawlerReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				job.setInputFormatClass(TextInputFormat.class);
				FileOutputFormat.setOutputPath(job, outputPath);
				TextInputFormat.setMaxInputSplitSize(job, Long.MAX_VALUE);

				// Create the workload for reducers
				writeInputFile(fs, tasksInputPath, fromNo, toNo,
						nbRefIDsPerFetch);
				TextInputFormat.addInputPath(job, tasksInputPath);

				// Run the job
				if (job.waitForCompletion(true)) {
					returnCode = Errors.NONE.getValue();
				} else {
					returnCode = Errors.JOB_COMPLETION.getValue();
				}

				// Do the cleaning
				fs.delete(tasksInputPath, true);
			} else {
				returnCode = Errors.CANNOT_CREATE_SAVING_FOLDER.getValue();
			}
		}

		return returnCode;
	}
}
