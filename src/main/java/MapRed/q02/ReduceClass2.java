package MapRed.q02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceClass2 extends Reducer<LongWritable, Text, LongWritable, Text> {
    LongWritable avg = new LongWritable();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
        int counter = 0;
        for(Text val : value){
            counter++;
            if(counter <= 10){
                context.write(key, val);
            }
        }
    }
}
