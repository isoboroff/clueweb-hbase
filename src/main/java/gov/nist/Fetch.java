package gov.nist.clueweb-hbase;

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Fetch {
    public static void main(String[] args) throws IOException {

	if (args.length != 2) {
	    System.out.println("Usage: Fetch [table] [URL or docid]\n");
	    System.exit(-1);
	}

	Configuration config = HBaseConfiguration.create();
	HTable table = new HTable(config, args[0]);

	String query = args[1];
	if (query.startsWith("http://")) {
	    query = Util.reverse_hostname(query);
	} else if (query.startsWith("clueweb")) {
	    Get g = new Get(Bytes.toBytes(query));
	    Result r = table.get(g);
	    byte[] value = r.getValue(Bytes.toBytes("meta"),
				      Bytes.toBytes("url"));
	    query = Bytes.toString(value);
	}

	Get g = new Get(Bytes.toBytes(query));
	Result r = table.get(g);
	byte[] value = r.getValue(Bytes.toBytes("content"), 
				  Bytes.toBytes("raw"));
	System.out.println(Bytes.toString(value));
    }
}
