package app;

import java.io.UnsupportedEncodingException;
import java.util.Stack;

import org.codehaus.jettison.json.JSONObject;

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

	public static enum availableProtocols {
		HTTP("http"), HTTPS("https");

		private String value;

		private availableProtocols(String value) {
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
	 * Ask Sphinxer for statistics.
	 * Can take these parameters:
	 *	command, project, qcanonical_seal, tkey_from, tkey_to, unit
	 * 
	 * @return the JSON query result
	 */
	public static JSONObject askForStats() {
		JSONObject jsonRes = new JSONObject();

		// TODO: implement if needed
		// Example URL:
		// https://www.wikileaks.org/plusd/sphinxer_do.php?command=stats_from_query&project=all_cables&qcanonical=&qcanonical_seal=7fa94db3387685fe93c1c13cdca27a62&tkey_from=19660000&tkey_to=20091200&unit=month

		return jsonRes;
	}

	/**
	 * Ask Sphinxer for documents ref_id listing.
	 * Can take these parameters:
	 *	token, qlimit, qsort, (qtto), (qtfrom),
	 *	qcanonical_seal, project, command, format
	 * 
	 * @return the JSON query result
	 * @throws UnsupportedEncodingException 
	 */
	public static Stack<String> askForRefIDListing(int fromDocumentNo,
			int nbOfDocuments) throws UnsupportedEncodingException,
			IllegalArgumentException {
		Stack<String> refIdStack = new Stack<String>();

		int qlimit = nbOfDocuments;
		qlimit = Math.min(qlimit, 500);
		qlimit = Math.max(qlimit, 20);
		if (qlimit != nbOfDocuments) {
			throw new IllegalArgumentException(
					"nbOfDocuments should be between 20 and 500");
		}

		StringBuilder URLStr = new StringBuilder();
		URLStr.append(Sphinxer.availableProtocols.HTTP.getValue());
		URLStr.append(Sphinxer.urlStr);

		QueryString parameters = new QueryString();
		parameters.add(Token.key, String.valueOf(fromDocumentNo));
		parameters.add(QLimit.key, String.valueOf(qlimit));
		parameters.add(Sphinxer.Sorts.key, Sphinxer.Sorts.TIME_ASC.getValue());
		parameters.add(Sphinxer.CanonicalSeals.key,
				Sphinxer.CanonicalSeals.PUBLIC_READ.getValue());
		parameters.add(Sphinxer.Projects.key,
				Sphinxer.Projects.ALL_CABLES.getValue());
		parameters.add(Sphinxer.Cmds.key,
				Sphinxer.Cmds.DOC_LIST_FROM_QUERY.getValue());
		parameters.add(Sphinxer.Format.key, Sphinxer.Format.HTML.getValue());

		URLStr.append(parameters.toString());

		// TODO: Fetch URLStr
		// TODO: Extract ref_ids from it

		return refIdStack;
	}
}
