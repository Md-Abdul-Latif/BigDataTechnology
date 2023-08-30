package MapRed.q03;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, CompositeKey, Text, CompositeKey> {

    @Override
    public void reduce(Text key, Iterable<CompositeKey> values, Context context) throws IOException, InterruptedException{
        String productid = " ";
        String out = " ";
        int count = 0;
        Text proddetails = new Text();
        CompositeKey ck = new CompositeKey();

        for(CompositeKey val : values){
            out = val.getProdictid() + val.getProduct_title();
            productid = val.getProdictid() + val.getProduct_title();
            count += val.getCount();
        }
        ck.setCount(count);
        ck.setProdictid(productid);
        proddetails.set(out);

        context.write(key, ck);
    }
}
