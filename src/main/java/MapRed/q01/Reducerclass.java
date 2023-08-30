package MapRed.q01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducerclass extends Reducer<Text, IntWritable, Text, LongWritable> {
    LongWritable sum = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        long count = 0;
        for(IntWritable val : values){
            count += val.get();
        }
        sum.set(count);
        context.write(key, sum);
    }
}
