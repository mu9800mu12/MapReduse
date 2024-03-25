package cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    /**
     * 부모 매퍼 자바 파일에 작성된 맵 함수를 덮어쓰기 수행
     * 맵 함수는 분석할 파일의 래코드 1줄마다 실행됨
     * 파일의 라인수가 100개라면 맵 함수는 100번 실행됨
     */

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //분석할 파일의 한 줄 값
        String line =value.toString();

        //단어 빈도수 구현은 단어가 아닌 것을 기준으로 단어로 구분함
        // 분석 한 줄 내용을 단어가 아닌 것으로 나눔
        // word 변수는 다억 ㅏ저장됨
        for (String word : line.split("\\W+")) {

            // word의 글자가 3글자 이상인 단어만 빈도수 세기
            if (word.length() > 3) {

                //대소문자 구분을 없애기 위해 모두 대문자로 변경하기
                word = word.toLowerCase();

                //Suffle and Sort로 데이털를 전달하기
                // 전달하는 값은 단어와 빈도수(1)를 전달함
                context.write(new Text(word), new IntWritable(1));

            }
        }


    }

}
