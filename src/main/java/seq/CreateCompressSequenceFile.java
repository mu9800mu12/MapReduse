package seq;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import success.ResultCount;

@Log4j
public class CreateCompressSequenceFile extends Configuration implements Tool {
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
        configuration.set("AppName", "Compress Sequence File Create Test");

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
    public int run(String[] args) throws  Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName" + appName);

        // 실행하기 위한 잡 객체 가져오기
        Job job = Job.getInstance(conf);

        // 잡이 시작되는 메인함수 존재 파일 설정
        job.setJarByClass(CreateCompressSequenceFile.class);

        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path (args[1]));

        // 시퀀스 파일 구조로 변환하기
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 생성될 파일을 압출할 때 사용할 알고리즘 선택(SnappyCodec 사용)
        FileOutputFormat.setCompressOutput(job, true);

        // 압축 방식 설정(BLOCK마다 압축, RECORD : 한줄마다 압축)
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

        // 시퀀스 파일 만들기는 리듀서 작업은 필요하지 않음
        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);



    }




}
