import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Application1 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession
                .builder()
                .appName("TP Employees Spark SQL Dataformes csv")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df =ss.read().option("header",true).csv("employes.csv");

        //df.printSchema();
        //df.filter(col("age").between(30,35)).show();
        //df.select(col("departement"),col("salary").cast("double")).groupBy("departement").avg("salary").show();
        //df.select(col("departement"),col("salary").cast("double")).groupBy("departement").count().show();
        //df.select(max(col("salary").cast("double"))).show();

    }
}
