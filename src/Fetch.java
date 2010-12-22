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

    protected static String reverse_hostname(String uri) {
	URL url = null;
	try {
	    url = new URL(uri);
	} catch (MalformedURLException mue) {
	    return null;
	}
	String host = url.getHost();
	StringBuilder newhost = new StringBuilder();
	String[] parts = host.split("\\.", 0);
	for (int i = parts.length - 1; i > 0; i--) {
	    if (i > 0)
		newhost.append(parts[i]).append(".");
	}
	newhost.append(parts[0]);
	int port = url.getPort();
	if (port != -1)
	    newhost.append(":").append(port);
	newhost.append(url.getFile());
	return newhost.toString();
    }

    public static void main(String[] args) throws IOException {

	if (args.length != 1) {
	    System.out.println("Usage: Fetch [URL or docid]\n");
	    System.exit(-1);
	}

	Configuration config = HBaseConfiguration.create();
	HTable table = new HTable(config, "webtable");

	String query = args[0];
	if (query.startsWith("http://")) {
	    query = reverse_hostname(query);
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
