package app;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Stack;

import utils.PageLoader;
import utils.Sphinxer;

public class Crawler {

	public static File outputDir = new File("pages-crawled");
	public static String downloaderURL = "http://search.wikileaks.org/plusd/cables";
	public static Boolean gzCompressionInOutput = true;
	public static String extension = gzCompressionInOutput ? "gz" : "html";
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

	public static Stack<Map.Entry<Integer, Integer>> getWorkList(int fromNo,
			int toNo, int stepsSize) {

		Stack<Map.Entry<Integer, Integer>> workList = new Stack<Map.Entry<Integer, Integer>>();
		for (int tmpFrom = fromNo; tmpFrom <= toNo; tmpFrom += stepsSize) {
			int tmpTo = Math.min(tmpFrom + stepsSize - 1, toNo);
			workList.push(new AbstractMap.SimpleEntry<Integer, Integer>(
					tmpFrom, tmpTo));
		}

		return workList;
	}

	public static Stack<String> getRefIDs(
			Stack<Map.Entry<Integer, Integer>> workList, File saveDir,
			boolean gzCompression) {

		Stack<String> refIDs = new Stack<String>();
		for (Map.Entry<Integer, Integer> fromToPair : workList) {
			int from = fromToPair.getKey();
			int to = fromToPair.getValue();

			try {
				Stack<String> tmpRefIDs = Sphinxer.askForRefIDListing(from, (to
						- from + 1));
				for (String refID : tmpRefIDs) {
					String fileName = refID + "." + extension;
					File outputFile = new File(saveDir, fileName);
					if (!outputFile.exists()) {
						refIDs.push(refID);
					}
				}
			} catch (IllegalArgumentException e) {
				// We can't fix this error, stop the program
				e.printStackTrace();
			}
		}

		return refIDs;
	}

	public static void main(String[] args) {
		Errors returnCode = Errors.UNASSIGNED_ERROR_CODE;

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
				returnCode = Errors.BAD_ARGS;
			}

			if (Sphinxer.isValidDocumentNo(fromNo) != 0) {
				System.out
						.println(String
								.format("Argument <from-documentNo> `%d` is not in range (%d, %d)",
										fromNo, Sphinxer.minDocumentNo,
										Sphinxer.maxDocumentNo));
				returnCode = Errors.BAD_ARGS;
			} else if (Sphinxer.isValidDocumentNo(toNo) != 0) {
				System.out
						.println(String
								.format("Argument <to-documentNo> `%d` is not in range (%d, %d)",
										toNo, Sphinxer.minDocumentNo,
										Sphinxer.maxDocumentNo));
				returnCode = Errors.BAD_ARGS;
			}
		}

		System.out.println(String.format(
				"Starting the crawl of documents [%d, %d]", fromNo, toNo));

		// Create the saving dir
		if (returnCode == Errors.UNASSIGNED_ERROR_CODE) {
			if (!outputDir.exists()) {
				try {
					outputDir.mkdir();
				} catch (SecurityException se) {
					returnCode = Errors.CANNOT_CREATE_SAVING_FOLDER;
				}
			}
		}

		if (returnCode == Errors.UNASSIGNED_ERROR_CODE) {
			// Run the crawler
			for (String refID : getRefIDs(
					getWorkList(fromNo, toNo, nbRefIDsPerFetch), outputDir,
					gzCompressionInOutput)) {
				int nbOfTries = 1;
				boolean success = false;
				while (!success) {
					// Download and save the page associated with refID
					try {
						File outputFile = new File(outputDir, refID + "."
								+ extension);
						String urlStr = Crawler.downloaderURL + "/" + refID
								+ ".html";
						PageLoader.getAndSave(outputFile, urlStr,
								Crawler.gzCompressionInOutput);

						success = true;
						System.out.println(String.format(
								"Page(refID: %s) fetched", refID));
					} catch (IOException e) {
						System.out
								.println(String
										.format("Cannot fetch page(refID: %s), nbOfTries(%d).. retrying",
												refID, nbOfTries));
						nbOfTries++;
					}
				}
			}

			returnCode = Errors.NONE;
		}

		System.out.println(String.format("Exiting with status (%d)",
				returnCode.getValue()));
		System.exit(returnCode.getValue());
	}
}
