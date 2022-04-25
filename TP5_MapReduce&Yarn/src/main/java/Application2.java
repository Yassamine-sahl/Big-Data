import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application2 {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("nbr des employes ");
        conf.setJarByClass(Application2.class);

        conf.setMapperClass(EmployeMapper.class);
        conf.setReducerClass(NbrDesEmployeReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(DoubleWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path("employes.csv"));
        FileOutputFormat.setOutputPath(conf,new Path("./Output2"));

        JobClient.runJob(conf);
    }
}
