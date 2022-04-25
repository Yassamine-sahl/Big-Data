package ma.enset.TPMapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class TotalVentesReduce extends MapReduceBase
implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        Double somme = 0.00;
        while (values.hasNext()) {
            somme += values.next().get();
        }
        outputCollector.collect(key, new DoubleWritable(somme));
    }
}
