package MapRed.q02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DriverClass {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // First Mapreduce
        JobControl jobControl = new JobControl("jobChain");
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(DriverClass.class);
        job1.setJobName("MR1");

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.setMapperClass(MapperClass.class);
        job1.setReducerClass(ReducerClass.class);
        job1.setCombinerClass(ReducerClass.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(CompositeKey.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);
        jobControl.addJob(controlledJob1);

        // Second MapReduce
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(DriverClass.class);
        job2.setJobName("MR2");

        job2.setMapperClass(MapperClass2.class);
        job2.setReducerClass(ReduceClass2.class);
        job2.setReducerClass(ReduceClass2.class);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setNumReduceTasks(5);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        FileSystem fs = FileSystem.get(conf1);
        fs.delete(new Path(args[1]), true);

        //make job2 dependend on job1
        controlledJob2.addDependingJob(controlledJob1);
        //add the job to the jbo control
        jobControl.addJob(controlledJob2);

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
