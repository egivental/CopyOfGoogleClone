package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.storage.pagerank.PRRDSController;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.crypto.spec.PSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class WriterReducer extends Reducer<IntWritable, Text, Text, Text> {

    private PRRDSController prrdsController = new PRRDSController();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<String[]> prs = new ArrayList<>();

        for (Text value : values) {
            String[] p = value.toString().split("\\|");

            if ((p[0].startsWith("http") || p[0].startsWith("https")) && !p[0].contains("\"") && !p[0].contains("'")) {
                prs.add(p);
                context.write(new Text(p[0]), new Text(p[1]));

            }
        }

        System.out.println(key.toString() + "," + prs.size());
        if (!prs.isEmpty()) {
            try {
                prrdsController.addPageRanks(prs);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }


    }
}
