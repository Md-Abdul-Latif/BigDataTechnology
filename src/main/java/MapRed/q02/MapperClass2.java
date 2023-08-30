package MapRed.q02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass2 extends Mapper<LongWritable, Text, LongWritable, Text> {
    LongWritable avg = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split("\t");
        Text prd = new Text(tokens[0]);

        long rating_avg = Long.parseLong(tokens[2]);
        avg.set(rating_avg);

        context.write(avg, prd);
    }
}
