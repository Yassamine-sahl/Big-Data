import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("TP1 Exercie 3").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("1780.csv");

        //***********************MAX***************************//
        JavaRDD<String>rddLMAX = rdd1.filter(s -> s.split(",")[2].equals("TMAX"));
        JavaRDD<Integer> rddMAX = rddLMAX.map(a-> Integer.parseInt(a.split(",")[3]));

        Double tmpMaxElevee= Double.valueOf(rddMAX.reduce((v1, v2)->(Math.max(v1,v2))));
        System.out.println("Température MAX la plus élevée"+tmpMaxElevee);

        Double tmpMoyMAX = Double.valueOf(rddMAX.reduce((v1,v2)-> (v1+v2)) /rddMAX.count());
        System.out.println("Température MAX moyenne"+tmpMoyMAX);
        //****************************************************//

       //***********************MIN***************************//
        JavaRDD<String>rddLMIN = rdd1.filter(s -> s.split(",")[2].equals("TMIN"));
        JavaRDD<Integer> rddMIN = rddLMIN.map(a-> Integer.parseInt(a.split(",")[3]));

        Double tmpMinBasse= Double.valueOf(rddMAX.reduce((v1, v2)->(Math.min(v1,v2))));
        System.out.println("Température MIN la plus basse"+tmpMinBasse);

        Double tmpMoyMIN = Double.valueOf(rddMIN.reduce( (v1,v2)-> (v1+v2)) / rddMAX.count() );
        System.out.println("Température MIN moyenne"+tmpMoyMIN);
        //**********************************************************//

        //***********************STATIONS***************************//
        JavaPairRDD<String,Integer> tmpByStation = rdd1.mapToPair(s->new Tuple2<>(s.split(",")[0] ,Integer.parseInt(s.split(",")[3])) );

        List<Tuple2<String,Integer>> stationsSorted = new ArrayList<>();
        stationsSorted.addAll(tmpByStation.collect()); //
        stationsSorted.sort((Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) -> o2._2.compareTo(o1._2));

        System.out.print("Top 5 des stations météo les plus chaudes : ");
        ArrayList<String> top5 =new ArrayList<>() ;
        for (int i =0 ; i<stationsSorted.size() ; i++ ){
            if(!top5.contains(stationsSorted.get(i)._1))
                top5.add(stationsSorted.get(i)._1) ;
            if(top5.size()==5)
                break;
        }
        for (String s : top5)
            System.out.print(s+ " ");

        System.out.print("\nTop 5 des stations météo les plus froides: " );
        top5.clear();
        for (int i =stationsSorted.size()-1; i>=0 ; i-- ){
            if(!top5.contains(stationsSorted.get(i)._1))
                top5.add(stationsSorted.get(i)._1) ;
            if(top5.size()==5)
                break;
        }
        for (String s : top5)
            System.out.print(s+ " ");
        System.out.println();
        //**********************************************************//

    }
}









