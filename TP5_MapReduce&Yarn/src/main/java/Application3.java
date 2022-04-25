import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application3 {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("TMP min et max par mois");
        conf.setJarByClass(Application3.class);

        conf.setMapperClass(TempiratureMapper.class);
        conf.setReducerClass(TempiratureReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path("02907099999.csv"));
        FileOutputFormat.setOutputPath(conf,new Path("./Output3"));

        JobClient.runJob(conf);
    }
}
