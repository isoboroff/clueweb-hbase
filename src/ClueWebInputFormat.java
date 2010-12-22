import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ArrayDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class ClueWebInputFormat extends FileInputFormat<LongWritable, Text> {

    public static final Log LOG =
	LogFactory.getLog(ClueWebInputFormat.class);

    @Override
    public boolean isSplitable(JobContext job, Path filename) {
	return false;
    }
    
    @Override 
    public RecordReader<LongWritable, Text> 
	createRecordReader(InputSplit split, TaskAttemptContext context)
	throws IOException {
	ClueWebRecordReader rr = new ClueWebRecordReader();
	rr.initialize(split, context);
	return rr;
    }

    // The following are incomprehensibly private in FileInputFormat...
    private static final PathFilter hiddenFileFilter = new PathFilter(){
	    public boolean accept(Path p){
		String name = p.getName(); 
		return !name.startsWith("_") && !name.startsWith("."); 
	    }
	}; 
    
    /**
     * Proxy PathFilter that accepts a path only if all filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    private static class MultiPathFilter implements PathFilter {
	private List<PathFilter> filters;
	
	public MultiPathFilter(List<PathFilter> filters) {
	    this.filters = filters;
	}
	
	public boolean accept(Path path) {
	    for (PathFilter filter : filters) {
		if (!filter.accept(path)) {
		    return false;
		}
	    }
	    return true;
	}
    }

    
    @Override
	protected List<FileStatus> listStatus(JobContext job) 
	throws IOException {
	Path[] dirs = getInputPaths(job);
	if (dirs.length == 0) {
	    throw new IOException("No input paths specified in job");
	}

	List<FileStatus> result = new ArrayList<FileStatus>();
	List<IOException> errors = new ArrayList<IOException>();
	ArrayDeque<FileStatus> stats = new ArrayDeque<FileStatus>(dirs.length);

	// creates a MultiPathFilter with the hiddenFileFilter and the
	// user provided one (if any).
	List<PathFilter> filters = new ArrayList<PathFilter>();
	filters.add(hiddenFileFilter);
	PathFilter jobFilter = getInputPathFilter(job);
	if (jobFilter != null) {
	    filters.add(jobFilter);
	}
	PathFilter inputFilter = new MultiPathFilter(filters);

	// Set up traversal from input paths, which may be globs
	for (Path p: dirs) {
	    FileSystem fs = p.getFileSystem(job.getConfiguration());
	    FileStatus[] matches = fs.globStatus(p, inputFilter);
	    if (matches == null) {
		errors.add(new IOException("Input path does not exist: " + p));
	    } else if (matches.length == 0) {
		errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
	    } else {
		for (FileStatus globStat: matches) {
		    stats.add(globStat);
		}
	    }
	}

	while (!stats.isEmpty()) {
	    FileStatus stat = stats.pop();
	    if (stat.isDir()) {
		FileSystem fs = stat.getPath().getFileSystem(job.getConfiguration());
		for (FileStatus sub: fs.listStatus(stat.getPath(), 
						   inputFilter)) {
		    stats.push(sub);
		} 
	    } else {
		result.add(stat);
	    }
	}

	if (!errors.isEmpty()) {
	    throw new InvalidInputException(errors);
	}
	LOG.info("Total input paths to process : " + result.size()); 
	return result;
    }
	

    public static class ClueWebRecordReader 
	extends RecordReader<LongWritable, Text> {
	private long start;
	private long end;
	private long pos;
	private Path path;
	private LineRecordReader in;
	private LongWritable cur_key = null;
	private Text cur_val = null;

	public ClueWebRecordReader() {
	}
	    

	public void initialize(InputSplit split, TaskAttemptContext context)
	    throws IOException {
	    cur_key = new LongWritable(0);
	    try {
		if (split instanceof FileSplit) {
		    path = ((FileSplit)split).getPath();
		} else {
		    path = new Path("");
		}
		start = 0;
		pos = 0;
		end = split.getLength();
		in = new LineRecordReader();
		in.initialize(split, context);
	    } catch (InterruptedException ie) {
		throw new IOException(ie);
	    }
	}
    
	public LongWritable getCurrentKey() { 
	    return cur_key;
	}
	public Text getCurrentValue() { 
	    return cur_val; 
	}

	private Text hold = null;
	private long last_pos = 0;
	StringBuilder buf = null;

	public synchronized boolean nextKeyValue() 
	    throws IOException {
	    Text line = null;
	    cur_val = new Text();
	    boolean in_doc = false;

	    if (buf == null)
		buf = new StringBuilder();

	    if (hold != null) {
		buf.append(hold.toString()).append("\n");
		hold = null;
		in_doc = true;
	    }
	    
	    try {
		while (in.nextKeyValue()) {
		    line = in.getCurrentValue();
		    int size = line.getLength();
		    last_pos = pos;
		    pos += size;

		    if (line.find("WARC/0.18") == 0) {
			if (in_doc) {
			    in_doc = false;
			    hold = line;
			    break;
			} else {
			    in_doc = true;
			    continue;
			}
		    }

		    if (in_doc)
			buf.append(line.toString()).append("\n");
		}
	    } catch (java.io.IOException e) {
	    }

	    if (buf.length() > 0) {
		cur_val.set(buf.toString());
		cur_key.set(cur_key.get() + 1);
		buf = null;
		return true;
	    } else {
		LOG.info("nkv returning false");
		return false;
	    }
	}

	public float getProgress() {
	    return Math.min(1.0f, (pos) / (float)(end));
	}

	public synchronized void close() throws IOException {
	    if (in != null) {
		in.close();
	    }
	}
    }



}
