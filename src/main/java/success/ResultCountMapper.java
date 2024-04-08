package success;


import java.io.IOException;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

@Log4j
public class ResultCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 맵리듀스 잡 이름
    String appName = "";

    // URL 전송 성공 여부 코드값
    // 성공 : 200 / 실패 : 200외 다른 값
    String resultCode = "";


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        // 사용자 정의 정보 가져오기
        Configuration conf = context.getConfiguration();

        // 드라이버 정의된 맵리듀스 잡 이름 가져오기
        this.appName = conf.get("appName");

        // 드라이버에 정위돈 환경설정값 가져오기
        // 없으면 200으로 설정
        this.resultCode = conf.get("resultCode");

        log.info("[" + this.appName + "] 난 map함수 실행하기 전에 1번만 실행되는 setup 함수다!");

    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);

        log.info("[" + this.appName + "] 난 에러나도 무조건 실행되는 cleanup 함수다!!");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        //분석할 한 줄 값
        String line = value.toString();

        //단어별로 나누기
        String[] arr = line.split("\\W+");

        int pos = arr.length -2;

        String result = arr[pos];

        log.info("[" + this.appName + "]" + result);

        // 드라이버 파일에서 정위한 코드값과 로그의 코드값이 일치한다면
        if (resultCode.equals(result)) {
            context.write(new Text(result), new IntWritable(1));

        }
    }

}