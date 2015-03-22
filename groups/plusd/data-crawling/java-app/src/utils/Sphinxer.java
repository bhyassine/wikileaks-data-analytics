package utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Sphinxer is the web interface for wikileaks cables.
 * 
 * Parameters of the query found:
 * 
 * 	format			[optional]	The response format
 * 	command			[mandatory]	The type of query
 * 	project			[mandatory]	The associated dataset
 * 	qcanonical		[optional]	(?)
 * 	qcanonical_seal	[mandatory]	(?) For each query rights-level, a security seal is associated
 * 	qtfrom			[optional]	From date (timestamp)
 * 	qtto			[optional]	To date (timestamp)
 * 	tkey_from		[optional]	From date (timestamp)
 * 	tkey_to			[optional]	To date (timestamp)
 * 	qsort			[optional]	results sorting type
 * 	qlimit			[optional]	Cardinality of results. Min = 20, Max = 500
 * 	token			[mandatory]	! The document ID [1, 2325959] !
 * 	unit			[optional]	The level of aggregation
 */
public class Sphinxer {

	public static String urlStr = "www.wikileaks.org/plusd/sphinxer_do.php";

	// The documentsNo are consecutive integers.
	// They have been tried at hand to deduct a (min,max)
	public static int minDocumentNo = 1;
	public static int maxDocumentNo = 2325960;

	public static enum AvailableProtocols {
		HTTP("http"), HTTPS("https");

		private String value;

		private AvailableProtocols(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static enum Cmds {
		STATS_FROM_QUERY("stats_from_query"), DOC_LIST_FROM_QUERY(
				"doc_list_from_query");

		private String value;
		public static String key = "command";

		private Cmds(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static enum Projects {
		ALL_CABLES("all_cables");

		private String value;
		public static String key = "project";

		private Projects(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static enum CanonicalSeals {
		PUBLIC_READ("7fa94db3387685fe93c1c13cdca27a62");

		private String value;
		public static String key = "qcanonical_seal";

		private CanonicalSeals(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static enum Units {
		DAY("day"), MONTH("month");

		private String value;
		public static String key = "unit";

		private Units(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static enum Sorts {
		TIME_ASC("tasc");

		private String value;
		public static String key = "qsort";

		private Sorts(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	public static class QLimit {
		public static String key = "qlimit";
		public static int minLimit = 20;
		public static int maxLimit = 500;
	}

	public static class Token {
		public static String key = "token";
	}

	/**
	 * Warning, the possible query formats depends
	 * on the sphinxer cmds.
	 * 
	 * e.g. sphinxerCmds.STATS_FROM_QUERY only accepts sphinxerFormat.JSON
	 * 
	 * Note that html provides less information in some cases and hence
	 * gives a smaller response.
	 * Note that html format gives a JSON with a .content html field
	 */
	public static enum Format {
		JSON("json"), HTML("html");

		private String value;
		public static String key = "format";

		private Format(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}
	}

	/**
	 * Is the documentNo a valid one?
	 * 
	 * @param no The document number
	 * @return (-1, 0, 1) if it is respectively (below, in, above) the valid interval
	 */
	public static int isValidDocumentNo(int no) {
		if (no < minDocumentNo) {
			return -1;
		} else if (maxDocumentNo < no) {
			return 1;
		}

		return 0;
	}

	/**
	 * Ask Sphinxer for documents ref_id listing.
	 * Can take these parameters:
	 *	token, qlimit, qsort, (qtto), (qtfrom),
	 *	qcanonical_seal, project, command, format
	 * 
	 * @return the JSON query result
	 * @throws IOException 
	 * @throws MalformedURLException 
	 * @throws JSONException 
	 */
	public static Stack<String> askForRefIDListing(int fromDocumentNo,
			int nbOfDocuments) throws IllegalArgumentException {
		Stack<String> refIdStack = new Stack<String>();

		int qlimit = nbOfDocuments;
		qlimit = Math.min(qlimit, QLimit.maxLimit);
		qlimit = Math.max(qlimit, QLimit.minLimit);
		if (qlimit != nbOfDocuments) {
			throw new IllegalArgumentException(String.format(
					"nbOfDocuments should be between %d and %d",
					QLimit.minLimit, QLimit.maxLimit));
		}
		int validityCode = isValidDocumentNo(fromDocumentNo);
		if (validityCode == -1) {
			throw new IllegalArgumentException(String.format(
					"The documents begin from No %d (and not below)",
					minDocumentNo));
		}
		if (validityCode == 1) {
			throw new IllegalArgumentException(
					String.format("The documents end at No %d (and not above)",
							maxDocumentNo));
		}

		// Build the URL
		StringBuilder URLStr = new StringBuilder();
		URLStr.append(AvailableProtocols.HTTP.getValue());
		URLStr.append("://");
		URLStr.append(urlStr);
		URLStr.append('?');

		QueryString parameters = new QueryString();
		parameters.add(Token.key, String.valueOf(fromDocumentNo));
		parameters.add(QLimit.key, String.valueOf(qlimit));
		parameters.add(Sorts.key, Sphinxer.Sorts.TIME_ASC.getValue());
		parameters.add(CanonicalSeals.key,
				CanonicalSeals.PUBLIC_READ.getValue());
		parameters.add(Projects.key, Projects.ALL_CABLES.getValue());
		parameters.add(Cmds.key, Cmds.DOC_LIST_FROM_QUERY.getValue());
		parameters.add(Format.key, Format.HTML.getValue());

		URLStr.append(parameters.toString());

		// Download the page
		String pageStr = "";
		try {
			pageStr = PageLoader.get(URLStr.toString());
		} catch (MalformedURLException e1) {
			// Internal error, stop program
			e1.printStackTrace();
		} catch (IOException e2) {
			// Internal error, stop program
			e2.printStackTrace();
		}

		JSONObject jsonObj;
		String content = "";
		try {
			jsonObj = new JSONObject(pageStr);
			content = jsonObj.getString("content");
		} catch (JSONException e) {
			// We have to stop, no plan-B
			e.printStackTrace();
		}

		// Extract ref_ids
		// e.g.: <tr id="72TEHRAN1164_a">
		Pattern p = Pattern.compile("<tr id=\"([A-Za-z0-9_]+?)\">");
		Matcher m = p.matcher(content);

		while (m.find()) {
			refIdStack.push(m.group(1));
		}

		return refIdStack;
	}
}
