package combiner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IPCount2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String ip = "";
        int forCnt = 0;

        for (String field : line.split("\\W+")) {

            if (field.length() > 0) {

                forCnt++;
                ip += (field + "."); // Ip 값 저장함

                // ip 는 192.168.0.127 등등 ==> 4가지 숫자로 조합되기에 반복문은 1번부터 4번까지 IP 주소임
                if (forCnt == 4) {

                    ip = ip.substring(0, ip.length() - 1);

                    context.write(new Text(ip), new IntWritable(1));
                }
            }
        }
    }
}
