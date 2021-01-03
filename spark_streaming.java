import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import twitter4j.Status;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class spark_streaming  {

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir", "C:/winutils");

        //Setting the system properties twitter4j.oauth.consumerKey, twitter4j.oauth.consumerSecret,
        // twitter4j.oauth.accessToken and twitter4j.oauth.accessTokenSecret to connect to the twitter API
        System.setProperty("twitter4j.oauth.consumerKey", "************************************************");
        System.setProperty("twitter4j.oauth.consumerSecret", "************************************************");
        System.setProperty("twitter4j.oauth.accessToken", "************************************************");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "************************************************");

        SparkConf conf = new SparkConf()
                .setAppName("LSDA_Assignment5") //Appname to uniquely identify a job in cluster(isn't much relevant to our task here)
                .setMaster("local[4]").set("spark.executor.memory", "4g"); //4 core processor to work individually with 1 gigabyte of heap memory

        //Creating object of JavaStreamingContext and setting the batch duration to 1000 so as to receive tweets in batches of 1 second
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        Logger.getRootLogger().setLevel(Level.ERROR);
        jssc.checkpoint(Files.createTempDir().getAbsolutePath());

        /*##########################################################################################################################*/

        /*
        Creating a input stream that returns tweets received from twitter using twitter4J's oauth tokens
        Reference: https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/streaming/twitter/TwitterUtils.html
        */
        JavaDStream<Status> tweets =  TwitterUtils.createStream(jssc);
        /*.filter(status->status.getLang().equals("en"));*///Uncomment this line of code to filter out only english language tweets while creating the stream

        //Printing a sample	of the tweets received from Twitter every second
        JavaDStream<String> statuses = tweets.map(status -> status.getText());
        statuses.print();

        /*##########################################################################################################################*/

        /*
        Creating a Dstream<String, String> containing every Tweet and the total number of words in that tweet.
        Here I'm splitting each status/tweet based on spaces(" ") to obtain a list of all words in the tweet and taking the size() of it to get count of words
        */
        JavaPairDStream<String, String> total_words = statuses
                .mapToPair( status -> new Tuple2<String, String>("Status/Tweet:" + status, "Total number of words in the tweet: " + Arrays.asList(status.split(" ")).size()));
        total_words.print();

        /*
        Creating a Dstream<String, String> containing every Tweet and the total number of characters in that tweet.
        Here I'm splitting each status/tweet on "" to obtain a list of all characters in the tweet and taking the size() of it to get count of words
        */
        JavaPairDStream<String, String> total_characters = statuses
                .mapToPair(status -> new Tuple2<String, String>("Status/Tweet:" + status,"Total number of Characters in Tweet: " + Arrays.asList(status.split("")).size()));
        total_characters.print();

        /*
        Creating a DStream<String> which contains all the hashtags in the given batch
        Here I'm splitting each status/tweet based on spaces(" ") to obtain a list of all words in the tweet
        and then filtering out all the words that start with #  which are hashtags
        */
        JavaDStream<String> hashTags = statuses.flatMap(f -> Arrays.asList(f.split(" ")).iterator())
                .filter(s -> s.startsWith("#"));
        hashTags.print();

        //We can use the below code as well to print the hashtags in each tweet (displaying Tweet and the hashtags in it)
        /*JavaPairDStream<String, List<String>> hashTags =  statuses
                .mapToPair(status -> new Tuple2<String, List<String>>("Status/Tweet:" + status + " HashTags: ", Arrays.stream(status.split(" ")).filter(x -> x.startsWith("#")).collect(Collectors.toList())));
        hashTags.print();*/

        /*##########################################################################################################################*/

        // counting the	average	number of words and characters per tweet

        //Iterating through each rdd in the dstream
        statuses.foreachRDD(rdd -> {
            //Getting the tweet count and checking if it is greater than 0 to avoid division by zero error.
            long statuses_count = rdd.count();
            if(statuses_count >0)
            {
                /*
                References : http://yuanxu-li.github.io/technical/2018/06/10/reduce-and-fold-in-spark.html
                Splitting the tweets by ""(for characters) and " "(for words) and mapping them to get the tweet length and reducing to sum the counts.
                */
                long words_count = rdd.map(status -> status.split(" ")).map(b -> b.length).reduce((Integer i1, Integer i2) -> i1 + i2);
                long characters_count = rdd.map(status -> status.split("")).map(b -> b.length).reduce((Integer i1, Integer i2) -> i1 + i2);

                //We could use the below code as well to get count of words and characters, the output is tested for both the code snippets
                /*long words_count = rdd.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator()).count();
                * long characters_count  = rdd.flatMap((String x) -> Arrays.asList(x.split("")).iterator()).count();*/

                //To fetch the average number of words/characters, I'm dividing the total number of words/characters by total number of tweets
                System.out.println("Average Number of Words per tweet are: " + words_count / statuses_count );
                System.out.println("Average Number of characters per tweet are: " + characters_count / statuses_count );
            }
        });


        //counting the	top	10	hashtags

        //mapping the hashtags
        JavaPairDStream<String, Integer> mapped_hashtags = hashTags
                .mapToPair(str -> new Tuple2<String, Integer>(str, 1));

        //reducing the hashtags by key
        JavaPairDStream<String, Integer> reduced_hashtags = mapped_hashtags.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

        //To sort by the number of hashtags on key we are swapping <String, Integer> to <Integer, String>, then Sorting the hashtags by taking key(i.e the count) of (key,value) pair in descending order
        JavaPairDStream<Integer, String> sorted_hashtags = reduced_hashtags
                .map(a -> a.swap()).mapToPair(tuple -> new Tuple2<Integer, String>(tuple._1(),tuple._2()))
                .transformToPair((Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>) tuple -> tuple.sortByKey(false));

        //Iterating through each rdd in the dstream
        sorted_hashtags.foreachRDD(rdd ->
        {
            if(rdd.count()>0)
            {
                //Printing the top	10	hashtags
                System.out.println("The top 10 hashtags are:");
                rdd.take(10).forEach(System.out::println);
            }
        });

        /*##########################################################################################################################*/

        //Creating a new DStream which is computed based on windowed batches of the source DStream
       JavaDStream<String> statuses_with_window = statuses.window(new Duration(5 * 60 * 1000), new Duration(30 * 1000));

        //Iterating through each rdd in the dstream
       statuses_with_window
               .foreachRDD(rdd -> {
                   //Getting the tweet count and checking if it is greater than 0 to avoid division by zero error.
                    long statuses_count = rdd.count();
                    if(statuses_count>0)
                    {
                        /*
                        References : http://yuanxu-li.github.io/technical/2018/06/10/reduce-and-fold-in-spark.html
                        Splitting the tweets by ""(for characters) and " "(for words) and mapping them to get the tweet length and reducing to sum the counts.
                        */
                        long words_count = rdd.map(a -> a.split(" ")).map(a -> a.length).reduce((Integer i1, Integer i2) -> i1 + i2);
                        long characters_count = rdd.map(b -> b.split("")).map(a -> a.length).reduce((Integer i1, Integer i2) -> i1 + i2);

                        //Could use the below code as well to get count of words and characters
                        /*long words_count = rdd.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator()).count();
                         * long characters_count  = rdd.flatMap((String x) -> Arrays.asList(x.split("")).iterator()).count();*/

                        System.out.println("Average Number of Words per tweet in the given window are: " + words_count / statuses_count);
                        System.out.println("Average Number of characters per tweet in the given window are: " + characters_count / statuses_count);
                    }});

        /*
        Reference : https://spark.apache.org/docs/latest/streaming-programming-guide.html
        Creating a  new DStream of (K, V) pairs where the values for each key are aggregated using
        the given reduce function func over batches in a sliding window.
        */
        JavaPairDStream<String, Integer> reduced_hashtags_with_window = mapped_hashtags.reduceByKeyAndWindow((Integer i1, Integer i2) -> i1 + i2
                , new Duration(5 * 60 * 1000), new Duration(30 * 1000));

        //To sort by the number of hashtags on key we are swapping <String, Integer> to <Integer, String>, then Sorting the hashtags by taking key(i.e the count) of (key,value) pair in descending order
        JavaPairDStream<Integer, String> sorted_hashtags_with_window = reduced_hashtags_with_window
                .map(f -> f.swap()).mapToPair(tuple -> new Tuple2<Integer, String>(tuple._1(),tuple._2()))
                .transformToPair((Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>) tuple -> tuple.sortByKey(false));

        //Iterating through each rdd in the dstream
        sorted_hashtags_with_window.foreachRDD(rdd ->
        {
            if(rdd.count()>0)
            {
                //Printing the top	10	hashtags
                System.out.println("The top 10 hashtags in the given window are:");
                rdd.take(10).forEach(System.out::println);
            }
        });

        /*##########################################################################################################################*/

        jssc.start();
        jssc.awaitTermination();
    }
}
