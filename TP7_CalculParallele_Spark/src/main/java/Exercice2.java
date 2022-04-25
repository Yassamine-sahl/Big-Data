import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Exercice 2").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("ventes.txt");

        JavaPairRDD<String,Double> rdd2=rdd1
                .mapToPair(s-> new Tuple2<>(s.split(" ")[1],Double.parseDouble(s.split(" ")[3])));

        JavaPairRDD<String,Double>  rdd3=rdd2.reduceByKey((v1, v2)->v1+v2);
        System.out.println("les ventes par une ville");
        rdd3.foreach(a-> System.out.println(a));

        JavaPairRDD<String,Double> rdd4=rdd1
                .mapToPair( s->new Tuple2<>(s
                        .split(" ")[2] +" " +  s.split(" ")[1],Double.parseDouble(s.split(" ")[3])));
        JavaPairRDD<String,Double>  rdd5=rdd4.reduceByKey((v1, v2)->v1+v2);
        System.out.println("les ventes par une ville pour une année ");
        rdd5.foreach(a-> System.out.println(a));



        //JavaRDD<String> rdd2=rdd1.flatMap(s->Arrays.asList(s.split(" ")[1]).iterator());
        //JavaRDD<String> rdd3=rdd1.flatMap(s->Arrays.asList(s.split(" ")[3]).iterator());
        //rdd2.foreach(s-> System.out.println(s));
        //rdd4.foreach(nameTuple-> System.out.println(nameTuple._1()+" "+nameTuple._2()));
    }
}
