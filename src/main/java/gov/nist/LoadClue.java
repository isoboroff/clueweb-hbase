/*
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nist.clueweb-hbase;

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Sample Uploader MapReduce
 * <p>
 * This is EXAMPLE code.  You will need to change it to work for your context.
 * <p>
 * Uses {@link TableReducer} to put the data into HBase. Change the InputFormat 
 * to suit your data.  In this example, we are importing a CSV file.
 * <p>
 * <pre>row,family,qualifier,value</pre>
 * <p>
 * The table and columnfamily we're to insert into must preexist.
 * <p>
 * There is no reducer in this example as it is not necessary and adds 
 * significant overhead.  If you need to do any massaging of data before
 * inserting into HBase, you can do this in the map as well.
 * <p>Do the following to start the MR job:
 * <pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapreduce.SampleUploader /tmp/input.csv TABLE_NAME
 * </pre>
 * <p>
 * This code was written against HBase 0.21 trunk.
 */
public class LoadClue {

    private static final String NAME = "LoadClue";
  
    private static HashMap<String, String> get_headers(String doc) {
	HashMap<String, String> hdr = new HashMap(20);
	try {
	    BufferedReader in = new BufferedReader(new StringReader(doc));
	    int nl = 0;
	    String line = null;
	    while ((line = in.readLine()) != null) {
		if (line.length() == 0)
		    nl++;
		if (nl == 2)
		    break;
		int i = line.indexOf(':');
		if (i == -1)
		    continue;
		try {
		    hdr.put(line.substring(0, i), line.substring(i+2));
		} catch (Exception e) {}
	    }
	    StringBuilder buf = new StringBuilder();
	    while ((line = in.readLine()) != null) {
		buf.append(line).append('\n');
	    }
	    hdr.put("document", buf.toString());
	} catch (IOException e) {}
	return hdr;
    }

    protected static String table_name = null;

    protected static void setTableName(String n) {
	table_name = n;
    }

    static class Uploader 
	extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	private long checkpoint = 1000;
	private long count = 0;
    
	@Override
	    public void map(LongWritable key, Text value, Context context)
	    throws IOException {

	    String raw = value.toString();
	    HashMap<String, String> parse = get_headers(raw);

	    if (parse.get("WARC-Type").equals("response")) {
		String uri = parse.get("WARC-Target-URI");
		if (uri == null) {
		    System.err.println("Doc has no target-uri");
		    return;
		}
		String keystr = Util.reverse_hostname(uri);

		byte[] row = Bytes.toBytes(keystr);
		byte[] family = Bytes.toBytes("content");
		byte[] qualifier = Bytes.toBytes("raw");
		byte[] val = Bytes.toBytes(parse.get("document"));

		// Create Put
		Put put = new Put(row);
		put.add(family, qualifier, val);
      
		// Uncomment below to disable WAL. This will improve
		// performance but means you will experience data loss in
		// the case of a RegionServer crash.
		put.setWriteToWAL(false);
		
		String trecid = parse.get("WARC-TREC-ID");
		byte[] row2 = null;
		Put put2 = null;
		if (trecid != null) {
		    row2 = Bytes.toBytes(parse.get("WARC-TREC-ID"));
		    put2 = new Put(row2);
		    byte[] fam2 = Bytes.toBytes("meta");
		    byte[] qual2 = Bytes.toBytes("url");
		    byte[] val2 = row;
		    put2.add(fam2, qual2, val2);
		    put2.setWriteToWAL(false);
		}

		try {
		    context.write(new ImmutableBytesWritable(row), put);
		    if (trecid != null)
			context.write(new ImmutableBytesWritable(row2), put2);
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
		
		// Set status every checkpoint lines
		if(++count % checkpoint == 0) {
		    context.setStatus("Emitting doc " + count);
		}
	    }
	}

	@Override 
	    public void cleanup(Context context) 
	    throws IOException {
	    if (LoadClue.table_name == null)
		return;
	    context.setStatus("Sending flush");
	    HBaseAdmin admin = new HBaseAdmin(context.getConfiguration());
	    admin.flush(LoadClue.table_name);
	}
    }
  
    /**
     * Job configuration.
     */
    public static Job configureJob(Configuration conf, String [] args)
	throws IOException {
	Path inputPath = new Path(args[0]);
	String tableName = args[1];
	Job job = new Job(conf, NAME + "_" + tableName);
	job.setJarByClass(Uploader.class);
	FileInputFormat.setInputPaths(job, inputPath);
	job.setInputFormatClass(ClueWebInputFormat.class);
	job.setMapperClass(Uploader.class);
	LoadClue.setTableName(tableName);

	// No reducers.  Just write straight to table.  Call initTableReducerJob
	// because it sets up the TableOutputFormat.
	TableMapReduceUtil.initTableReducerJob(tableName, null, job);
	TableMapReduceUtil.addDependencyJars(conf, TableOutputFormat.class);
	job.setNumReduceTasks(0);
	return job;
    }

    /**
     * Main entry point.
     * 
     * @param args  The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if(otherArgs.length != 2) {
	    System.err.println("Wrong number of arguments: " + otherArgs.length);
	    System.err.println("Usage: " + NAME + " <input> <tablename>");
	    System.exit(-1);
	}
	Job job = configureJob(conf, otherArgs);
	boolean result = job.waitForCompletion(true);
	System.out.println("Flushing table " + otherArgs[1]);
	HBaseAdmin admin = new HBaseAdmin(conf);
	admin.flush(otherArgs[1]);
	System.exit((result == true) ? 0 : 1);
    }
}
