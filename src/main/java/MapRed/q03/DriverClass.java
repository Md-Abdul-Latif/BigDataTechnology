package MapRed.q03;

import MapRed.q01.Driverclass;
import MapRed.q01.Mapperclass;
import MapRed.q01.Reducerclass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DriverClass {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        Configuration conf = new Configuration();
//
//        Job job = Job.getInstance(conf, "Question3");
//        job.setJarByClass(DriverClass.class);
//
//        job.setMapperClass(MapperClass.class);
//        job.setCombinerClass(ReducerClass.class);
//        job.setReducerClass(ReducerClass.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(CompositeKey.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(CompositeKey.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
////		Thread jobControlThread = new Thread(jobControl);
////		jobControlThread.start();
//
//        FileSystem fs = FileSystem.get(conf);
//        fs.delete(new Path(args[1]), true);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Question03");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator","\t");

        // Driver Class
        job.setJarByClass(Driverclass.class);

        job.setInputFormatClass(TextInputFormat.class);

        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);


        // Mapper
        job.setMapperClass(Mapperclass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer
        job.setReducerClass(Reducerclass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Create only 1 reducer
        job.setNumReduceTasks(1);
        FileSystem fs = FileSystem.get(job.getConfiguration());

        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }

        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
