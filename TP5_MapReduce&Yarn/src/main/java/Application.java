import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("salaire min et max");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(EmployeMapper.class);
        conf.setReducerClass(EmployeReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path("employes.csv"));
        FileOutputFormat.setOutputPath(conf,new Path("./Output"));

        JobClient.runJob(conf);
    }
    }

