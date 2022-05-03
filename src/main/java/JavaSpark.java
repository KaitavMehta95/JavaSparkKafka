
import org.apache.spark.sql.SparkSession;

public class JavaSpark {
    public static void main(String[] args) {

        System.out.println("Spark Hello World");
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application").getOrCreate();

        // Read messages from kafka

        // deploy it from jenkins
        // write aiflow script to deploy it and create table
        // OOP structure
        // Make connection to bq - read vault
        // Write messages to file / big query
    }
}