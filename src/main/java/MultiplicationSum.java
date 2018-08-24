import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultiplicationSum {
    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: line number
            // inputValue: user:movieB \t rating
            // outputKey: user:movieB
            // outputValue: rating
            String[] line = value.toString().trim().split("\t");
            String outputKey = line[0];
            String outputValue = line[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }
}
