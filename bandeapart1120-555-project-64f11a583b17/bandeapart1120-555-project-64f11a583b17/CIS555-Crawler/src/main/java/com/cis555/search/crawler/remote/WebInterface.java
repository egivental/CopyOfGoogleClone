package com.cis555.search.crawler.remote;

import static spark.Spark.get;
import static spark.Spark.port;

public class WebInterface {


    public static void main(String[] args) {
        RemoteMaster remoteMaster = new RemoteMaster();

        port(8081);
        get("/", (request, response) -> {
            response.redirect("/crawler/list", 301);
            return null;
        });

        get("/crawler/list", (request, response) -> {
            String header = "<head><title>Crawler List</title></head>\r\n";
            StringBuffer sb = new StringBuffer();

            sb.append("<h2>Crawlers Status</h2>");
            sb.append("<table>");
            sb.append("<tr> <th>Client</th> <th>Address</th> <th>Status</th> <th>HTMLCounts</th><th>Last Update</th></tr>");
            long curr = System.nanoTime();
            for (WorkerInfo workerInfo : remoteMaster.getWorkerInfoList()) {
                remoteMaster.requestStatistics(workerInfo.name);
                sb.append("<tr>");
                sb.append("<td>" + workerInfo.name + "</td>");
                sb.append("<td>" + workerInfo.addr + "</td>");
                sb.append("<td>" + workerInfo.status + "</td>");
                if (workerInfo.statistics == null) {
                    sb.append("<td>Not available</td>");
                } else {
                    sb.append("<td>" + workerInfo.statistics.counts.stream().mapToLong(Long::longValue).sum() + "</td>");
                }
                sb.append("<td>" + (curr - workerInfo.lastTimestamp) / 1000000000.0 + "s ago</td>");
                sb.append("</tr>");
            }
            sb.append("</table>");
            String statusPage = sb.toString();
            return header + "<body>" + statusPage;
        });
    }

}