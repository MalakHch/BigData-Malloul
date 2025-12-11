package spark.streaming.tp21;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;

public class Stream {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Stream <hostname> <port>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingWordCount")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap(
                        (String line) -> Arrays.asList(line.split("\\s+")).iterator(),
                        Encoders.STRING()
                );

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        query.awaitTermination();
    }
}
