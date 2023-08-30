package MapRed.q07;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class apachemahoutDemo {
    public static void main(String[] args) throws Exception {
        File userFile = new File("/home/hduser/backup/big-data/Big_Data_Java/Big-Data-Technology/AapcheMahout/in_mahout.csv");
        DataModel dataModel = new FileDataModel(userFile);

        UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);
        UserNeighborhood userNeighborhood = new ThresholdUserNeighborhood(0.2, userSimilarity, dataModel);
        // Create a generic user based recommender with the dataModel, the userNeighborhood and the userSimilarity
        Recommender genericRecommender =  new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);

        // Recommend 5 items for each user
        for (LongPrimitiveIterator iterator = dataModel.getUserIDs(); iterator.hasNext();)
        {
            long userId = iterator.nextLong();

            // Generate a list of 5 recommendations for the user
            List<RecommendedItem> itemRecommendations = genericRecommender.recommend(userId, 3);

            System.out.format("User Id: %d%n", userId);

            if (itemRecommendations.isEmpty())
            {
                System.out.println("No recommendations for this user.");
            }
            else
            {
                // Display the list of recommendations
                for (RecommendedItem recommendedItem : itemRecommendations)
                {
                    System.out.format("Recommened Item Id %d. Strength of the preference: %f%n", recommendedItem.getItemID(), recommendedItem.getValue());
                }
            }
        }

    }
}
