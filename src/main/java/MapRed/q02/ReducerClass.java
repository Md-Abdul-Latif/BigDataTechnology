package MapRed.q02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerClass extends Reducer<Text, CompositeKey, Text, CompositeKey> {
    CompositeKey ck = new CompositeKey();

    @Override
    protected void reduce(Text key, Iterable<CompositeKey> values, Context context) throws IOException, InterruptedException{
        long sum = 0;
        long count = 0;
        long avg = 0;
        for(CompositeKey val : values){
            sum += val.getRating_avg() * val.getCount();
            count = count + val.getCount();
        }
        avg = sum / count;

        ck.setCount(count);
        ck.setRating_avg(avg);

        context.write(key, ck);
    }
}
