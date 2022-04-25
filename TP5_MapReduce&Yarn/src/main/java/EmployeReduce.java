import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class EmployeReduce extends MapReduceBase implements Reducer<Text, DoubleWritable,Text, Text> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Double min=0.00,s ;
        if(value.hasNext())
            min =value.next().get();
        while(value.hasNext()){
            s=value.next().get();
            if(min>s)
                min=s ;
        }
        Double max=0.00 ;
        if(value.hasNext())
            max =value.next().get();
        while(value.hasNext()){
            s=value.next().get();
            if(max<s)
                max=s ;
        }
        outputCollector.collect(key, new Text(" min  salaire: " + min.toString() + " max salaire :" + max.toString()));
    }
}
