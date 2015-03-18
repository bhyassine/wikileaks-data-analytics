import java.io.File;
import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class WikiLeakHTMLParser {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Bad number of arguments. Expected use : <program> <directory of HTML files> <output directory>");
			System.exit(1);
		}
		File inputDir = new File(args[0]);

		File[] htmlFiles = inputDir.listFiles();
		if (htmlFiles != null) {
			File outputDir = new File(args[1]);
			String outputAddPath = "";
			if (outputDir.exists()){
				// TODO
			} else {
				// TODO
			}
			for (int i = 0; i < htmlFiles.length; i++) {
				File htmlFile = new File(htmlFiles[i].getPath());
				Document doc = null;
				try {
					doc = Jsoup.parse(htmlFile,"UTF-8","");
					Elements elements = doc.select("div.s_val");
					for (Element elem : elements) {
						System.out.println(elem.text());
					}
				} catch (IOException e) {
					System.err.println("Could not open '" + htmlFile.getPath() + "'");
				}
			}
		} else {
			System.err.println("'" + args[0] + "' does not exist or is not a directory.");
		}
	}
}
