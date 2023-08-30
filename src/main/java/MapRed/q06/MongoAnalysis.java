package MapRed.q06;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class MongoAnalysis {
    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase database = mongoClient.getDatabase("amazonDB");
        MongoCollection<Document> collection = database.getCollection("HealthCare");
        Document document = new Document();
        int count = 0;
        BufferedReader objReader = null;
        try {
            String strCurrentLine;
            String[] HealthData;
            objReader = new BufferedReader(new FileReader(
                    "/home/hduser/backup/big-data/Big_Data_Java/Big-Data-Technology/amazonHealthCare.tsv"));
            while ((strCurrentLine = objReader.readLine()) != null){
                HealthData = strCurrentLine.split("\t");
                ObjectId id = new ObjectId();
                document.put("_id", id);
                document.put("product_id", HealthData[3]);
                document.put("review", HealthData[12]);
                document.put("verivied_purchase", HealthData[11]);
                collection.insertOne(document);
                //System.out.println(document.toJson());
                count++;
            }
            System.out.println(count);
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            try {
                if(objReader != null){
                    objReader.close();
                }
            } catch (IOException ex){
                ex.printStackTrace();
            }
        }
    }
}
