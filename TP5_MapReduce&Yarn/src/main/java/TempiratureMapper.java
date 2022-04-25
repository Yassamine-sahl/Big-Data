import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Date;

public class TempiratureMapper extends MapReduceBase implements Mapper<LongWritable, Text , Text , DoubleWritable> {


    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        String mois = value.toString().split("-")[1] ;
        Double tmp = Double.parseDouble(value.toString().split("\",\"")[13].replace(",",".")) ;
        outputCollector.collect(new Text(mois) , new DoubleWritable(tmp));
    }
}
