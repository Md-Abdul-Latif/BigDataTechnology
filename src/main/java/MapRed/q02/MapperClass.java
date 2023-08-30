package MapRed.q02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, CompositeKey> {
    Text product = new Text();
    CompositeKey ck = new CompositeKey();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split("\t");
        if(tokens[5].equals("product_title")){
            return;
        } else {
            product.set(tokens[5].trim());
        }
        if(tokens[7].equals("star_rating")){
            return;
        } else {
            long rate = Long.parseLong(tokens[7].trim());
            ck.setCount(1);
            ck.setRating_avg(rate);
        }
        context.write(product, ck);
    }
}
