import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Application {
    public static void main(String[] args) {
        SparkSession ss= SparkSession
                .builder()
                .appName("TP Employees Spark SQL Dataformes json")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row>df =ss.read().option("multiline",true).json("employes.json");

        //df.show();
        //df.filter(col("age").between(30,35)).show();
        //df.groupBy("departement").avg("salary").show();
        //df.groupBy("departement").count().show();
        //df.select(max("salary")).show();


        //df.createOrReplaceTempView("employes");
        //ss.sql("select name from employes where age BETWEEN 30 and 35").show();
        //ss.sql("select departement, avg(salary) as MoyenneDepartement from employes group by departement").show();
        //ss.sql("select departement, count(name) as NombreSalaries from employes group by departement").show();
        //ss.sql("select max(salary) from employes ").show();
    }
}
