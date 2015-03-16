package app;

import java.util.AbstractMap;
import java.util.Map;

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
 * 	qtfrom			[???]		From date (timestamp)
 * 	qtto			[???]		To date (timestamp)
 * 	tkey_from		[optional]	From date (timestamp)
 * 	tkey_to			[optional]	To date (timestamp)
 * 	qsort			[???]		results sorting type
 * 	qlimit			[???]		(?) cardinality of results
 * 	token			[???]		(?) The FROM of a LIMIT statement
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

		private Cmds(String value) {
			this.value = value;
		}

		public Map.Entry<String, String> getValue() {
			return new AbstractMap.SimpleEntry<String, String>("command",
					this.value);
		}
	}

	public static enum Projects {
		ALL_CABLES("all_cables");

		private String value;

		private Projects(String value) {
			this.value = value;
		}

		public Map.Entry<String, String> getValue() {
			return new AbstractMap.SimpleEntry<String, String>("project",
					this.value);
		}
	}

	public static enum CanonicalSeals {
		PUBLIC_READ("7fa94db3387685fe93c1c13cdca27a62");

		private String value;

		private CanonicalSeals(String value) {
			this.value = value;
		}

		public Map.Entry<String, String> getValue() {
			return new AbstractMap.SimpleEntry<String, String>(
					"qcanonical_seal", this.value);
		}
	}

	public static enum Units {
		DAY("day"), MONTH("month");

		private String value;

		private Units(String value) {
			this.value = value;
		}

		public Map.Entry<String, String> getValue() {
			return new AbstractMap.SimpleEntry<String, String>("unit",
					this.value);
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

		private Format(String value) {
			this.value = value;
		}

		public Map.Entry<String, String> getValue() {
			return new AbstractMap.SimpleEntry<String, String>("format",
					this.value);
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
	 * Ask Sphinxer for documents meta listing.
	 * Can take these parameters:
	 *	(?)
	 * 
	 * @return the JSON query result
	 */
	public static JSONObject askForMetaListing() {
		JSONObject jsonRes = new JSONObject();

		// TODO: implement

		return jsonRes;
	}
}
