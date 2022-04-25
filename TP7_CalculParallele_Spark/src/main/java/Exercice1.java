import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Exercice1 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP1 Exercie 1").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.parallelize(Arrays.asList("Yassamine","Amina","Oumayima","Salma","Hajar",
                "Yassine","Youssef","Yahya","Yassine","Yassamine","Said","Wissal","Akram"));
        JavaRDD<String> rdd2=rdd1.flatMap(s->Arrays.asList(s.split(",")).iterator());
        JavaRDD<String> rdd3=rdd2.filter(a -> a.contains("l"));
        JavaRDD<String> rdd4=rdd2.filter(a -> a.contains("a"));
        JavaRDD<String> rdd5=rdd2.filter(a -> a.contains("Y"));
        JavaRDD<String> rdd6=rdd3.union(rdd4);
        JavaRDD<String> rdd71=rdd5.map(a -> a+" BDCC");
        JavaPairRDD<String,Integer> rdd70=rdd71.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> rdd7=rdd70.reduceByKey((v1, v2)->v1+v2);
        JavaRDD<String> rdd81=rdd6.map(a -> a+" GLSID");
        JavaPairRDD<String,Integer> rdd80=rdd81.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> rdd8=rdd80.reduceByKey((v1, v2)->v1+v2);
        JavaPairRDD<String,Integer> rdd9=rdd7.union(rdd8);
        JavaPairRDD<String,Integer> rdd10=rdd9.sortByKey();

        //Affichage
        rdd1.foreach(s-> System.out.println(s));
        rdd2.foreach(s-> System.out.println(s));
        rdd3.foreach(s-> System.out.println(s));
        rdd4.foreach(s-> System.out.println(s));
        rdd5.foreach(s-> System.out.println(s));
        rdd6.foreach(s-> System.out.println(s));
        rdd71.foreach(s-> System.out.println(s));
        rdd70.foreach(s-> System.out.println(s));
        rdd7.foreach(s-> System.out.println(s));
        rdd81.foreach(s-> System.out.println(s));
        rdd80.foreach(s-> System.out.println(s));
        rdd8.foreach(s-> System.out.println(s));
        rdd9.foreach(s-> System.out.println(s));
         rdd10.foreach(s-> System.out.println(s));








        //JavaRDD<String> rdd2=rdd1.map(a -> a+1);
        //JavaRDD<Integer> rdd4=rdd1.union(rdd2);
        //affichqge de RDD4
        //rdd4.foreach(a-> System.out.println(a));
        //somme des elements de RDD3
        //int somme=rdd3.reduce((a, b) ->a+b );
        //System.out.println(somme);

    }
}
