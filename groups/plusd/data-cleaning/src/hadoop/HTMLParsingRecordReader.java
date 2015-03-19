package hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * Custom RecordReader for HTML Parsing.
 * Set the key as the pageID and the value "don't care".
 * Only one file is processed per map..
 * @author Florian Briant
 *
 */
public class HTMLParsingRecordReader extends RecordReader<Text, Text> {

	/**
	 * pageID.html of the HTML file.
	 */
	private Text key;
	/**
	 * Dummy temporary boolean to assure only one record (one file) is read.
	 */
	private boolean first = true;
	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text("");
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	/**
	 * Get the name of the HTML file from the split.
	 */
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Path file = split.getPath();
        key = new Text(file.toString());

    }

    /**
     * Is called only once, as it needs only one map.
     */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (first) {
			first = false;
			return true;
		} else {
			return false;
		}
	}

}
