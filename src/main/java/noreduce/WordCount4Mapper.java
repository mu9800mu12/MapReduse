package noreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCount4Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

        String line = value.toString(); // 분석할 파일의 한 줄 값


        for (String word : line.split("\\W+")) {

            if (word.length() > 0) {

                context.write(new Text(word), new LongWritable(1));
            }
        }
    }
}