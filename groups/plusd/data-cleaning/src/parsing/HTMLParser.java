package parsing;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import exception.InputFileIsDirectoryException;
import exception.InputNotDirectoryException;
import exception.OutputAlreadyExistsException;

/**
 * An HTML parser. Uses the library JSOUP to parse HTML files.
 * 
 * TODO Define output format
 * 
 * @author Florian Briant
 *
 */
public class HTMLParser {
	/**
	 * The parser function for one single HTML file.
	 * 
	 * @param input
	 *            the path of the input HTML file
	 * @param jsoupSelect
	 *            the JSOUP selection criteria
	 * @return text results of the selection criteria
	 * @throws IOException
	 *             if it could not open the input file.
	 */
	public static String parse(String input, String jsoupSelect)
			throws IOException {
		String res = "";
		File inputFile = new File(input);
		if (!inputFile.isDirectory()) {
			Document doc = null;
			doc = Jsoup.parse(inputFile, "UTF-8", "");
			Elements elements = doc.select(jsoupSelect);
			for (Element elem : elements) {
				res += elem.text() + ";";
			}
		} else {
			throw new InputFileIsDirectoryException("Input file '" + input
					+ "' is a directory.");
		}
		return res;
	}

	/**
	 * The parse function for one HTML file in an HDFS Hadoop FileSystem
	 * 
	 * @param input
	 *            the hdfs path of the input HTML file
	 * @param jsoupSelect
	 *            the JSOUP selection criteria
	 * @param fs
	 *            the hadoop FileSystem
	 * @return the text results of the selection criteria
	 * @throws IOException
	 *             if it could not open the HTML file
	 */
	public static String parseHDFS(String input, String jsoupSelect,
			FileSystem fs) throws IOException {
		String res = "";
		Path inputPath = new Path(input);
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(inputPath)));
		String htmlString = "";
		String line = "";
		line = br.readLine();
		while (line != null) {
			htmlString += line;
			line = br.readLine();
		}
		Document doc = Jsoup.parse(htmlString);
		Elements elements = doc.select(jsoupSelect);
		for (Element elem : elements) {
			res += elem.text() + ";";
		}
		return res;
	}

	/**
	 * Parses HTML files from a directory.
	 * 
	 * @param dir
	 *            the directory containing HTML files
	 * @param jsoupSelect
	 *            the JSOUP selection criteria
	 * @return a list containing for each HTML file the text results of the
	 *         selection criteria.
	 * @throws InputNotDirectoryException
	 *             if input is not a directory
	 * @throws IOException
	 *             if it could not open one of the input file.
	 */
	public static ArrayList<String> parseDir(String dir, String jsoupSelect)
			throws IOException {
		ArrayList<String> res = new ArrayList<String>();
		File inputDir = new File(dir);
		File[] htmlFiles = inputDir.listFiles();
		if (htmlFiles != null) {
			for (int i = 0; i < htmlFiles.length; i++) {
				res.add(parse(htmlFiles[i].getPath(), jsoupSelect));
			}
		} else {
			throw new InputNotDirectoryException("'" + dir
					+ "' is not a directory");
		}
		return res;
	}

	/**
	 * Parses HTML files from a directory and writes for each file the text
	 * results in a line of the output file.
	 * 
	 * @param dir
	 *            the directory containing HTML files
	 * @param output
	 *            the output file (must not exist)
	 * @param jsoupSelect
	 *            the JSOUP selection criteria
	 * @throws IOException
	 *             if it could not open one of the input file.
	 */
	public static void parseDirAndOutput(String dir, String output,
			String jsoupSelect) throws IOException {
		File outputFile = new File(output);
		if (!outputFile.exists()) {
			ArrayList<String> parseResults = parseDir(dir, jsoupSelect);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outputFile)));
			for (int i = 0; i < parseResults.size(); i++) {
				bw.write(parseResults.get(i));
				bw.newLine();
			}
			bw.close();
		} else {
			throw new OutputAlreadyExistsException("The output file '" + output
					+ "' already exists.");
		}
	}
}
