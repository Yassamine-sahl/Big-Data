import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class TempiratureReduce extends MapReduceBase implements Reducer<Text, DoubleWritable,Text,Text> {
    @Override
    public void reduce(Text key, Iterator<DoubleWritable> value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Double min=0.00,s ;
        Double max=0.00 ;
        if(value.hasNext()){
            min =value.next().get();
            max =value.next().get();
        }
        while(value.hasNext()){
            s=value.next().get();
            if(min>s)
                min=s ;
            if(max<s)
                max=s ;
        }
        outputCollector.collect(new Text("Le Mois " + key), new Text(" min TMP " + min.toString() + " max TMP " + max.toString()));
    }
}
