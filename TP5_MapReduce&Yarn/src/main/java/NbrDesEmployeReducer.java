import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class NbrDesEmployeReducer extends MapReduceBase implements Reducer<Text, DoubleWritable,Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int nbr = 0 ;
        while(value.hasNext()){
            nbr+=1;
            value.next();
        }
        outputCollector.collect(new Text(key +" nbr des employées : " ) , new IntWritable(nbr));
    }
}