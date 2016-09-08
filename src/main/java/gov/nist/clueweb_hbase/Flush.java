package gov.nist.clueweb_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Flush {
    private static final String NAME = "Flush";

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Wrong number of arguments: " + otherArgs.length);
            System.err.println("Usage: " + NAME + " <tablename>");
            System.exit(-1);
        }
        System.out.println("Flushing table " + otherArgs[0]);
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.flush(otherArgs[0]);
    }
}
