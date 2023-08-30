package MapRed.q03;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements Writable {
    private String prodictid;
    private String product_title;
    private long count;

    public String getProdictid(){
        return prodictid;
    }
    public void setProdictid(String prodictid){
        this.prodictid = prodictid;
    }
    public String getProduct_title() {
        return product_title;
    }
    public void setProduct_title(String product_title){
        this.product_title = product_title;
    }
    public long getCount() {
        return count;
    }
    public void setCount(long count){
        this.count = count;
    }
    public CompositeKey(){
        super();
    }
    public CompositeKey(String productid, String product_title, long count){
        super();
        this.prodictid = productid;
        this.product_title = product_title;
        this.count = count;
    }
    public void readFields(DataInput in) throws IOException{
        prodictid = in.readUTF();
        product_title = in.readUTF();
        count = in.readLong();
    }
    public void write(DataOutput out) throws IOException{
        out.writeUTF(prodictid);
        out.writeUTF(product_title);
        out.writeLong(count);
    }
    @Override
    public String toString(){
        return prodictid + "\t" + product_title + "\t" + count;
    }



}
