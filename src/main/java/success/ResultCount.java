package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class ResultCount extends Configuration implements Tool {

    //맵리듀스 실행 함수

    public static void main(String[] args) throws Exception{

    //파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일로 2개르 받음
    if (args.length != 2) {

        log.info("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 한다");
        System.exit(-1);
    }

    int exitCode = ToolRunner.run(new ResultCount(), args);

    System.exit(exitCode);

    }
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "Send Result");

        // URL 호출결과가 성공인 경우(200)와 실패(200이 아닌 것) 설정 값
        configuration.set("resultCode", "200");
    }

    @Override
    public Configuration getConf() {

        //맵리 듀스 전체에 적용될 변수를 정의할 때 사용
        Configuration conf = new Configuration();

        // 변수 정의
        this.setConf(conf);

        return conf;
    }

    @Override
    public int run(String[] args) throws  Exception{

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName" + appName);

        Job job = Job.getInstance(conf);

        job.setJarByClass(ResultCount.class);

        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path (args[1]));

        job.setMapperClass(ResultCountMapper.class);

        job.setReducerClass(ResultCountReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);



    }






}
