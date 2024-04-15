package maponly;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import success.ResultCount;

@Log4j
public class ImageCount extends Configuration implements Tool {

    //맵리듀스 실행 함수

    public static void main(String[] args) throws Exception{

        //파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일로 2개르 받음
        if (args.length != 2) {

            log.info("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 한다");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new ImageCount(), args);

        System.exit(exitCode);

    }
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "Send Result");

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

        // 캐시메모리에 올릴 분석 파일
        String analysisFile = "/access_log";

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName" + appName);

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        // 호출이 발생하면, 메모리에 저장하여 캐시 처리 수행
        // 하둡분산시스템에 저장된 파일만 가능함
        job.addCacheFile(new Path(analysisFile).toUri());

        // 맵리듀스 잡이 시작되는 메인 함수가 존재하는 파일 설정
        job.setJarByClass(ResultCount.class);

        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(analysisFile));

        FileOutputFormat.setOutputPath(job, new Path (args[0]));

        job.setMapperClass(ImageCountMapper.class);

        job.setNumReduceTasks(0);

        boolean success = job.waitForCompletion(true);

        if(success) {

            //맵리듀스의 카운터는 맵리듀스 실행된 결과에 대한 보고를 위해 활용하는 영역
            // 맵 분석 결과에 대한 결과를 카운터 영역에 저장

            // jpg를 요청한 URL 수
            long jpg = job.getCounters().findCounter("imageCount", "jpg").getValue();

            // gif
            long gif = job.getCounters().findCounter("imageCount", "gif").getValue();

            // jpg와 gif를 제외한 요청한 URL 수
            long other = job.getCounters().findCounter("imageCount", "other").getValue();

            log.info("jpg" + jpg);
            log.info("gif" + gif);
            log.info("other" + other);

            return 0;

        } else {
            return 1;

        }


    }






}
