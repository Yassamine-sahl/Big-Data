package ma.enset.TPMapReduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application2 {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf();
        conf.setJobName("Total des ventes par annee");
        conf.setJarByClass(Application2.class);

        conf.setMapperClass(TotalVentesAnneeMapperQ2.class);
        conf.setReducerClass(TotalVentesReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path("ventes.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("./OutputQ2"));

        JobClient.runJob(conf);
    }
}
