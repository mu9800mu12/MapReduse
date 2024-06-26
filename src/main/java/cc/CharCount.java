package cc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  맵리듀스를 실행하기 위한 메인 함수가 존재하는 자바 파일
 *  드라이버 파일로 부름
 */
public class CharCount {


    //맵리듀스 실행함수
    public static void main(String[] args) throws Exception {

        //파라미터 분석할 파일(폴더)와 분석 결과가 저장될 파일(폴더)로 2개를받음

         if(args.length != 2) {
             System.out.printf("분석할 폴더(파일) 및 분석결과가 저장될 폴더를 입력해야 합니다");
             System.exit(-1);
         }

         // 맵리듀스 실행을 위한 잡 객체를 가져오기
         // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job =Job.getInstance();


        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(CharCount.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확인할 때 편리함
        job.setJobName("Char Count");

        //분석할 폴더 --첫번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

           //분석 결과가 저장되는 폴더 -- 두번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

         // 맵리듀스의 맵 역할 수행하는 매퍼 자바 파일 설정
        job.setMapperClass(CharCountMapper.class);

         //  리듀스 자바파일 설정
        job.setReducerClass(CharCountReducer.class);

        // 분석결과가 저장될 때 사용될 키의 데이터 타입
        job.setOutputKeyClass(Text.class);

        // 분석 결과가 저장될 때 사용될 값의 데이터타입
        job.setOutputValueClass(IntWritable.class);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);




}
}
