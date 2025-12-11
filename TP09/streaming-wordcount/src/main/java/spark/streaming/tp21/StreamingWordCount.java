package spark.streaming.tp21;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Arrays;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StreamingWordCount <hostname> <port>");
            System.exit(1);
        }

        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf()
                .setAppName("StreamingWordCount")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000));

        JavaReceiverInputDStream<String> lines =
                jssc.socketTextStream(hostname, port);

        JavaDStream<String> words = lines.flatMap(
                line -> Arrays.asList(line.split("\\s+")).iterator()
        );

        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
