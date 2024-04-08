package tool;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount2 extends Configuration implements Tool {


    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception{

        // 파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일(폴더)로 2개를 받음.
        if (args.length != 2) {
            System.out.printf("분석할 폴더(파일) 및 분석결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }


        int exitCode = ToolRunner.run(new WordCount2(), args);

        System.exit(exitCode);


    }
    @Override
    public void setConf(Configuration configuration){

        configuration.set("AppName", "ToolRunner Test");
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

        System.out.println("appName : " + appName);

        Job job = Job.getInstance(conf);


        job.setJarByClass(WordCount2.class);

        job.setJobName(appName);

        FileInputFormat.setInputPaths(job, new Path(args[0]));


        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCount2Mapper.class);

        job.setReducerClass(WordCount2Reducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);
    }
}