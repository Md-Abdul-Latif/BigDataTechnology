package MapRed.q03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperClass extends Mapper<LongWritable, Text, Text, CompositeKey> {
    // Called once for each key/value pair in the input split
    IntWritable count = new IntWritable(1);

    Text customerid = new Text();
    CompositeKey ck = new CompositeKey();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split("\t");

        String customer_id = tokens[1];
        String product_id = tokens[3];
        String product_title = tokens[5];

        customerid.set(customer_id);

        ck.setProdictid(product_id);
        ck.setProduct_title(product_title);
        ck.setCount(1);

        context.write(customerid, ck);
    }

}
