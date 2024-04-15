package maponly;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImageCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //분석할 한 줄 값
        String[] fields = value.toString().split("\"");

        if (fields.length > 1) {
            String request = fields[1];
            fields = request.split(" ");

            if (fields.length > 1) {

                String fileName = fields[1].toLowerCase();

                if (fileName.endsWith(".jpg")) {
                    context.getCounter("imageCount", "jpg").increment(1);

                } else if (fileName.endsWith(".gif")) {
                    context.getCounter("imageCount", "gif").increment(1);

                } else {
                    context.getCounter("imageCount", "other").increment(1);
                }
            }

        }
    }
}