package main;

import java.io.IOException;

import parsing.HTMLParser;

/**
 * HTML Parser for Cables HTML files from WikiLeak. Use extern library JSOUP for
 * HTML parsing. Parses each Cables HTML files from an input directory, recovers
 * important information about the cable, and writes for each file the
 * information in a line of the specified output file.
 * For one file, output is of form : "info1;info2;info3".
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
			HTMLParser.parseDirAndOutput(args[0], args[1],"div.s_val");
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}
}
