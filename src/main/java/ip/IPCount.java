package ip;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPCount {

    public static void main(String[] args) throws Exception {


        //파라미터는 분석할 파일과 분석 결과가 저장될 파일로 2개를 바등ㅁ
        if (args.length != 2) {
            System.out.printf("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance();

        job.setJarByClass(IPCount.class);

        job.setJobName("IP Count");

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IPCountMapper.class);

        job.setReducerClass(IPCountReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }


}
