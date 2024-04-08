package combiner;

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
public class IPCount2 extends Configuration implements Tool {

    public static void main(String[] args) throws Exception {

        //파라미터는 분석할 파일과 분석 결과가 저장될 파일로 2개를 바등ㅁ
        if (args.length != 2) {
            log.info("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new IPCount2(), args);

        System.exit(exitCode);
    }

    @Override
    public void setConf(Configuration configuration) {

        configuration.set("AppName", "Combiner Test");

    }

    @Override
    public Configuration getConf() {

        Configuration conf = new Configuration();

        this.setConf(conf);

        return conf;
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName : " + appName);

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        job.setJarByClass(IPCount2.class);

        job.setJobName("Combiner IP Count2");

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IPCount2Mapper.class);

        job.setReducerClass(IPCount2Reducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }


}
