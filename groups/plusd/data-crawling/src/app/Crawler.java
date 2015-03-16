package app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.io.IOUtils;
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
import org.codehaus.jettison.json.JSONObject;

public class Crawler extends Configured implements Tool {

	public static Path datesInputPath = new Path("input-dates.txt");
	public static char newline = '\n';
	public static char separator = '\t';

	// Minimum/Maximum plusD available date
	// https://www.wikileaks.org/plusd/
	public static SimpleDateFormat defaultDF = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static String defaultFrom = "1966-01-01";
	public static String defaultTo = "2010-12-31";

	public static String errorToken = "error";

	// If there are more than 500 documents in one
	// interval, we will not be able to download
	// them all. That's actually a limitation
	// TODO: Implement a fallback method (planB) in this case?
	public static int datesIntervalDay = 42;

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

		public void map(Object lineObj, Text datesStr, Context context)
				throws IOException, InterruptedException {

			String[] datesArray = datesStr.toString().split(
					String.valueOf(separator));
			Date fromDate, toDate;
			String fromDateStr = datesArray[0];
			String toDateStr = datesArray[1];
			try {
				fromDate = defaultDF.parse(fromDateStr);
				toDate = defaultDF.parse(toDateStr);

				// Query type: GET
				// Protocol: http/https
				// URL: www.wikileaks.org/plusd/sphinxer_do.php
				// Function: The documents [token+1, token+qlimit] ordered
				// by qsort between qtfrom and qtto are returned
				//
				// Seems to be a minimum of 20 documents

				// Possible GET parameters:
				// ------------------------
				// format: {json (default), html}
				// command: doc_list_from_query
				// project: all_cables
				// qcanonical: empty / facultative (?)
				// qcanonical_seal: 7fa94db3387685fe93c1c13cdca27a62 (???)
				// [mandatory]
				// qtfrom: -125366400 [#seconds from 1 January 1970]
				// qtto: 1293839999 [#seconds from 1 January 1970]
				// qsort: tasc [means Time ascending, tdesc available]
				// qlimit: [0, 500]
				// token: 20

				// Json answer
				// ------------------------
				// {total_num_docs, doc_list, token, error, exec_time}
				//
				// total_num_docs: ???
				// doc_list: an array of objects { .., refid, .. }
				UriBuilder qUB = UriBuilder
						.fromPath("http://www.wikileaks.org")
						.path("plusd/sphinxer_do.php")
						.queryParam("format", "json")
						.queryParam("command", "doc_list_from_query")
						.queryParam("project", "all_cables")
						.queryParam("qcanonical_seal",
								"7fa94db3387685fe93c1c13cdca27a62")
						.queryParam("qtfrom", fromDate.getTime() / 1000)
						.queryParam("qtto", toDate.getTime() / 1000)
						.queryParam("qsort", "tasc");
				URL qURL = qUB.build().toURL();
				try {
					HttpURLConnection connection = (HttpURLConnection) qURL
							.openConnection();
					BufferedReader qBF = new BufferedReader(
							new InputStreamReader(connection.getInputStream()));

					JSONObject jsonResponse = new JSONObject(
							IOUtils.toString(qBF));

					String totalNumDocs = jsonResponse
							.getString("total_num_docs");

					// TODO: totalNumDocs are always 0..? check where the bug is
					// with the above query.
					// Suggestion: Check in depth how the queries work on
					// https://www.wikileaks.org/plusd/
					System.out.println(fromDateStr + " " + toDateStr + " ("
							+ String.valueOf(totalNumDocs) + ") "
							+ qURL.toString());

					qBF.close();
				} catch (IOException e) {
					// TODO: handle this case
					e.printStackTrace();
				} catch (Exception e) {
					// TODO: handle this case
					e.printStackTrace();
				}

				// TODO: outputs the pairs (page_id, ...) for each page_id of
				// documents found in the interval of the date chunk
				context.write(new Text("a"), new Text("b"));

			} catch (ParseException e) {
				context.write(
						new Text(errorToken),
						new Text(String.format("Unable to parse %s or %s",
								fromDateStr, toDateStr)));
			}
		}
	}

	public static class CrawlerReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text refid, Iterable<Text> dontCare, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			// TODO: delete
			System.out.println(refid.toString());

			// TODO: save the html file corresponding to its pair.page_id
			context.write(new Text("c"), new Text("d"));
		}
	}

	public static enum sphinxerCmds {
		STATS_FROM_QUERY("stats_from_query"), DOC_LIST_FROM_QUERY(
				"doc_list_from_query");

		private String value;

		private sphinxerCmds(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum sphinxerProjects {
		ALL_CABLES("all_cables");

		private String value;

		private sphinxerProjects(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum sphinxerCanonicalSeals {
		PUBLIC_READ("7fa94db3387685fe93c1c13cdca27a62");

		private String value;

		private sphinxerCanonicalSeals(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum sphinxerUnits {
		DAY("day"), MONTH("month");

		private String value;

		private sphinxerUnits(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	/**
	 * Warning, the possible query formats depends
	 * on the sphinxer cmds.
	 * 
	 * e.g. sphinxerCmds.STATS_FROM_QUERY only accepts sphinxerFormat.JSON
	 */
	public static enum sphinxerFormat {
		JSON("json"), HTML("html");

		private String value;

		private sphinxerFormat(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	/**
	 * Ask Sphinxer (the web interface for wikileaks cables)
	 * 
	 * Parameters of the query:
	 * 	format			[optional]	The response format
	 * 	command			[mandatory]	The type of query
	 * 	project			[mandatory]	The associated dataset
	 * 	qcanonical		[optional]	(?)
	 * 	qcanonical_seal	[mandatory]	(?) For each query rights-level, a security seal is associated
	 * 	qtfrom			[???]		From date (timestamp)
	 * 	qtto			[???]		To date (timestamp)
	 * 	tkey_from		[optional]	From date (timestamp)
	 * 	tkey_to			[optional]	To date (timestamp)
	 * 	qsort			[???]		results sorting type
	 * 	qlimit			[???]		(?) cardinality of results
	 * 	token			[???]		(?) The FROM of a LIMIT statement
	 * 
	 * @return the JSON query result
	 */
	public static JSONObject askSphinxer() {
		JSONObject jsonRes = new JSONObject();

		// TODO: implement

		return jsonRes;
	}

	public static boolean writeInputFile(FileSystem fs, Path path,
			Date fromDate, Date toDate, int intervalDay) throws IOException {

		// TODO: Use (*1) to get #documents by month, then split the dates
		// in a smart way
		// (*1) https://www.wikileaks.org/plusd/sphinxer_do.php?
		// -> command=stats_from_query&
		// -> project=all_cables&
		// -> qcanonical=&
		// -> qcanonical_seal=7fa94db3387685fe93c1c13cdca27a62&
		// -> tkey_from=19660000&
		// -> tkey_to=20091200&
		// -> unit=month
		OutputStreamWriter osw = new OutputStreamWriter(fs.create(path));

		StringBuilder dates = new StringBuilder();
		Calendar cal = Calendar.getInstance();
		boolean firstLine = true;

		cal.setTime(fromDate);
		Date tmpFrom = cal.getTime();

		cal.add(Calendar.DATE, intervalDay);
		Date tmpTo = cal.getTime();

		while (tmpTo.getTime() < toDate.getTime()) {
			if (!firstLine) {
				dates.append(newline);
			} else {
				firstLine = false;
			}
			String tmpFromStr = defaultDF.format(tmpFrom);
			String tmpToStr = defaultDF.format(tmpTo);
			dates.append(tmpFromStr + separator + tmpToStr);

			cal.add(Calendar.DATE, 1);
			tmpFrom = cal.getTime();

			cal.add(Calendar.DATE, intervalDay - 1);
			tmpTo = cal.getTime();
		}

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

		// Create & add to program dateInputFile
		if (writeInputFile(fs, datesInputPath, fromDate, toDate,
				datesIntervalDay)) {
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
