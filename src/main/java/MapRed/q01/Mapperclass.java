package MapRed.q01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapperclass extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Called once for each key/value pair in the input split
    IntWritable count = new IntWritable(1);
    String prodtitle;
    Text prod_title = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split("\t");
        if(tokens[5].equals("product_title")) {
            return;
        } else {
            prodtitle = tokens[5];
        }
        prod_title.set(prodtitle);
        context.write(prod_title, count);
    }

}
