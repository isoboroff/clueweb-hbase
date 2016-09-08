package gov.nist.clueweb_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Bench {
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: Bench [table] [inputfile]");
            System.exit(-1);
        }

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, args[0]);

        BufferedReader in = new BufferedReader(new FileReader(args[1]));
        String query = null;
        long c_count = 0;
        long m_count = 0;
        long start = System.currentTimeMillis();
        long last = start;

        while ((query = in.readLine()) != null) {
            try {
                if (query.startsWith("http://")) {
                    query = Util.reverse_hostname(query);
                } else if (query.startsWith("clueweb")) {
                    Get g = new Get(Bytes.toBytes(query));
                    Result r = table.get(g);
                    byte[] value = r.getValue(Bytes.toBytes("meta"),
                            Bytes.toBytes("url"));
                    query = Bytes.toString(value);
                    m_count++;
                }

                Get g = new Get(Bytes.toBytes(query));
                Result r = table.get(g);
                byte[] value = r.getValue(Bytes.toBytes("content"),
                        Bytes.toBytes("raw"));
                c_count++;

                if ((c_count % 10000) == 0) {
                    long now = System.currentTimeMillis();
                    double sec = (now - last) / 1000.0;
                    double rate = sec / 10000.0;
                    System.out.println("(" + c_count + ") 10,000 queries in " +
                            sec + "s (" + rate + " s/q)");
                    last = System.currentTimeMillis();
                }

            } catch (IOException e) {
                continue;
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Fetched " + c_count + " content records.");
        if (m_count > 0) {
            System.out.println("Fetched " + m_count + " meta records.");
        }
        System.out.println("Total time: " + (end - start) / 1000.0 + "s");
        System.out.println("Time per fetch: "
                + ((end - start) / ((c_count + m_count) * 1000.0))
                + "s");
    }
}
