package MapRed.q02;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable {
    private long count;
    private long rating_avg;

    public long getCount() {
        return count;
    }
    public void setCount(long count) {
        this.count = count;
    }
    public long getRating_avg(){
        return rating_avg;
    }
    public void setRating_avg(long rating_avg){
        this.rating_avg = rating_avg;
    }
    public CompositeKey(){
        super();
    }
    public CompositeKey(long count, long rating_avg){
        super();
        this.count = count;
        this.rating_avg = rating_avg;
    }
    public void readFields(DataInput in) throws IOException{
        count = in.readLong();
        rating_avg = in.readLong();
    }
    public void write(DataOutput out) throws IOException{
        out.writeLong(count);
        out.writeLong(rating_avg);
    }

    @Override
    public String toString(){
        return count + "\t" + rating_avg;
    }

}
