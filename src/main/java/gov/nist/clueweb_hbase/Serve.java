package gov.nist.clueweb_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Serve {

    protected static Configuration config;

    public static class FetchHandler extends AbstractHandler {

        protected String tabname;
        protected Configuration conf;
        protected HTable table;

        public FetchHandler(String table_name) throws IOException {
            tabname = table_name;
            conf = HBaseConfiguration.create();
            table = new HTable(conf, table_name);
        }

        public void handle(String target, HttpServletRequest req,
                           HttpServletResponse resp, int dispatch)
                throws IOException, ServletException {

            String query = target.substring(1);
            String url = null;

            System.err.println("FetchHandler.handle " + target);

            if (query.startsWith("http://")) {
                url = query;
                query = Util.reverse_hostname(query);
            } else if (query.startsWith("clueweb")) {
                Get g = new Get(Bytes.toBytes(query));
                Result r = table.get(g);
                byte[] value = r.getValue(Bytes.toBytes("meta"),
                        Bytes.toBytes("url"));
                query = Bytes.toString(value);
                url = Util.reverse_hostname(query);
            }

            Get g = new Get(Bytes.toBytes(query));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes("content"),
                    Bytes.toBytes("raw"));
            if (value != null) {
                // if we have it, ship it to the browser
                // attempt to tell the browser where it originally
                // came from.
                resp.setContentType("text/html");
                resp.setStatus(HttpServletResponse.SC_CREATED);
                if (url != null) {
                    resp.addHeader("Content-Location", url);
                }
                ((Request) req).setHandled(true);
                resp.getWriter().println(Bytes.toString(value));
            } else {
                // we don't have it... redirect to the original URL
                resp.setStatus(HttpServletResponse.SC_FOUND);
                resp.addHeader("Location", url);
                ((Request) req).setHandled(true);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        config = HBaseConfiguration.create();
        int port = 8888;
        if (args.length > 1)
            port = Integer.parseInt(args[1]);

        Server server = new Server(port);
        server.setHandler(new FetchHandler(args[0]));
        server.start();
        server.join();
    }

}
