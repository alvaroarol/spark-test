package com.ses.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;

public class SparkOnKafkaTest {

	public static void main(String[] args) throws StreamingQueryException, TimeoutException {
		
		// Schema of the retrieved Kafka records (array of records)
		StructType schema = new StructType(
			new StructField[] {
				new StructField("id", DataTypes.StringType, false, Metadata.empty()),
				new StructField("timeStamp", DataTypes.TimestampType, false, Metadata.empty()),
				new StructField("points", DataTypes.FloatType, false, Metadata.empty())
			}
		);
		ArrayType arrSchema = new ArrayType(schema, false);
		
		// Create Spark session
		SparkSession spark = SparkSession
				.builder()
				.appName("SparkOnKafkaTest")
				.getOrCreate();
		
		// Subscribe to the Kafka topic 'rawValues'
		Dataset<Row> rawData = spark
			.readStream()
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "rawValues")
			.option("startingOffsets", "earliest")
			.load();
		
		// Get the records (value) from the retrieved message
		Dataset<Row> jsonData = rawData.selectExpr("CAST(value AS STRING)");
		
		// Prepare columns
		Column explodedCols = explode(from_json(col("value"), arrSchema));
		
		// Structure retrieved records in columns
		Dataset<Row> records = jsonData.select(explodedCols).select("col.*");
		records.printSchema();
		
		// Processing columns
		Dataset<Row> processedRecords = records
			.groupBy("id")
			.agg(
				avg("points").as("averagePoints"),
				sum("points").as("totalPoints")
			)
			.drop("timeStamp")
			.drop("points");
		
		// Write back to kafka (only value, no key)
		processedRecords.selectExpr("to_json(struct(*)) AS value")
			.writeStream()
			.format("kafka")
			.outputMode("update") // Don't send all the averages for all IDs, only the ones with an updated average
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("topic", "averageValues")
			.option("checkpointLocation", "/tmp/spark-on-kafka-test/checkpoint")
			.start()
			.awaitTermination();

	}

}
