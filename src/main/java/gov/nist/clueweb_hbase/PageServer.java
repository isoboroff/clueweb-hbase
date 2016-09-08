package gov.nist.clueweb_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletHandler;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class PageServer {

    static HTable table;

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: RandomPageServer [table] [port]\n");
            System.exit(-1);
        }

        Configuration config = HBaseConfiguration.create();
        table = new HTable(config, args[0]);

        Server srv = new Server(Integer.parseInt(args[1]));
        HandlerCollection handlers = new HandlerCollection();
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(UrlServlet.class, "/clueweb12/url/*");
        handler.addServletWithMapping(ContentServlet.class, "/clueweb12/content/*");
        handlers.addHandler(handler);

		/*
        NCSARequestLog requestLog = new NCSARequestLog();
        requestLog.setFilename("/tmp/yyyy_mm_dd.request.log");
        requestLog.setFilenameDateFormat("yyyy_MM_dd");
        requestLog.setRetainDays(90);
        requestLog.setAppend(true);
        requestLog.setExtended(true);
        requestLog.setLogCookies(false);
        requestLog.setLogTimeZone("GMT");
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog(requestLog);
        handlers.addHandler(requestLogHandler);
		*/
        srv.setHandler(handlers);
        srv.start();
        srv.join();

        table.close();
    }

    @SuppressWarnings("serial")
    public static class UrlServlet extends HttpServlet {
        public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("text/html");
            resp.setStatus(HttpServletResponse.SC_OK);

            String query = req.getPathInfo().substring(1);

            Get g = new Get(Bytes.toBytes(query));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes("meta"), Bytes.toBytes("url"));

            String result_url = "http://" + Util.reverse_hostname("http://" + Bytes.toString(value));

            resp.getWriter().print(result_url);
        }
    }

    @SuppressWarnings("serial")
    public static class ContentServlet extends HttpServlet {
        public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            resp.setContentType("text/html");
            resp.setStatus(HttpServletResponse.SC_OK);

            String query = req.getPathInfo().substring(1);
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

            resp.getWriter().print(Bytes.toString(value));
        }
    }


}
