data = LOAD '/home/hduser/backup/big-data/Big_Data_Java/Big-Data-Technology/amazonHealthCare.tsv' AS (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date);

grouped = GROUP data by star_rating;

counted = FOREACH grouped GENERATE group, COUNT(data);

STORE counted into '/home/hduser/pig/BigTechnology/pigout';


