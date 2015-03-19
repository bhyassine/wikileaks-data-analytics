import hadoop.HTMLParsingMapReduce;




/**
 * HTML Parser for Cables HTML files from WikiLeak. Use extern library JSOUP for
 * HTML parsing. Parses each Cables HTML files from an input directory, recovers
 * important information about the cable, and writes it in a (key,value) pair from mapreduce framework.
 * For one file, output is of form : "pageID info1;info2;info3".
 * TODO specify output form
 * 
 * @author Florian Briant
 *
 */
public class WikiLeakCablesHTMLParser {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Bad number of arguments. Expected use : <program> <directory of HTML files> <output directory>");
			System.exit(1);
		}
		try {
			HTMLParsingMapReduce.launchMapReduce(args[0], args[1], "div.s_val");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
