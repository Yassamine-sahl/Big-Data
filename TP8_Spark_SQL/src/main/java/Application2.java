import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Application2 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession
                .builder()
                .appName("TP Employees Spark SQL DataSet json")
                .master("local[*]")
                .getOrCreate();

        Encoder<Employe> employeEncoder = Encoders.bean(Employe.class);

        Dataset<Employe> ds=ss.read().option("multiline",true).json("employes.json").as(employeEncoder);


        //ds.printSchema();
        //ds.show();
        //ds.filter((FilterFunction<Employe>) employe -> employe.getAge()>= 30 && employe.getAge()<= 35 ).show();
        //ds.groupBy("departement").avg("salary").show();
        //df.groupBy("departement").count().show();
        //df.select(max("salary")).show();
    }
}
