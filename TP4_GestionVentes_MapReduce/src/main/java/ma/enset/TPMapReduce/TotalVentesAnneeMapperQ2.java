package ma.enset.TPMapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class TotalVentesAnneeMapperQ2 extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        Double prix=Double.valueOf(value.toString().split(" ")[3]);
        String villeAnnee=value.toString().split(" ")[0]+" "+value.toString().split(" ")[1];
        outputCollector.collect(new Text(villeAnnee),new DoubleWritable(prix));
    }
}
