package success;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int resultCodeCount = 0;

        for(IntWritable value : values) {

            resultCodeCount += value.get();

        }

        // 분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(resultCodeCount));
    }


}
