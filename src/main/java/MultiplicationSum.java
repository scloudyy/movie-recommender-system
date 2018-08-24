import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultiplicationSum {
    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: line number
            // inputValue: user:movieB \t rating
            // outputKey: user:movieB
            // outputValue: rating
            String[] line = value.toString().trim().split("\t");
            String outputKey = line[0];
            String outputValue = line[1];
            context.write(new Text(outputKey), new DoubleWritable(Double.valueOf(outputValue)));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
}
