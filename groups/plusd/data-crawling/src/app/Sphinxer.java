package app;

import org.codehaus.jettison.json.JSONObject;

/**
 * Sphinxer is the web interface for wikileaks cables.
 * URL: 		www.wikileaks.org/plusd/sphinxer_do.php
 * Protocol: 	http/https
 */
public class Sphinxer {

	public static enum Cmds {
		STATS_FROM_QUERY("stats_from_query"), DOC_LIST_FROM_QUERY(
				"doc_list_from_query");

		private String value;

		private Cmds(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum Projects {
		ALL_CABLES("all_cables");

		private String value;

		private Projects(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum CanonicalSeals {
		PUBLIC_READ("7fa94db3387685fe93c1c13cdca27a62");

		private String value;

		private CanonicalSeals(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	public static enum Units {
		DAY("day"), MONTH("month");

		private String value;

		private Units(String cmdName) {
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
	public static enum Format {
		JSON("json"), HTML("html");

		private String value;

		private Format(String cmdName) {
			this.value = cmdName;
		}

		public String getValue() {
			return value;
		}
	}

	/**
	 * Ask Sphinxer
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
	public static JSONObject ask() {
		JSONObject jsonRes = new JSONObject();

		// TODO: implement

		return jsonRes;
	}
}
