import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class EmployeMapper extends MapReduceBase implements Mapper<LongWritable,Text ,Text,DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        String departement  = value.toString().split(",")[2] ;
        Double salaire = Double.parseDouble(value.toString().split(",")[4]) ;
        outputCollector.collect(new Text(departement),new DoubleWritable(salaire));
    }
}
