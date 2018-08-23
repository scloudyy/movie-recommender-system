import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CoOccurrenceMatrixNormalizer {

    public static class MatrixNomalizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: movie1:movie2
            // inputValue: relation
            // outputKey: movie1
            // outputValue: movie2:relation
            String[] moviesRelation = value.toString().trim().split("\t");
            String[] movies = moviesRelation[0].trim().split(":");

            context.write(new Text(movies[0]), new Text(movies[1] + ":" + moviesRelation[1]));
        }
    }

    public static class MatrixNomalizerRudecer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // inputKey: movie1
            // inputValue: movie2:relation
            // outputKey: movie2
            // outputValue: movie1:normalizeValue

            int sum = 0;
            Map<String, Integer> map = new HashMap<>(); // key: movie2, value: relation
            for (Text value : values) {
                String[] movieRelation = value.toString().trim().split(":");
                int relation = Integer.valueOf(movieRelation[1]);
                map.put(movieRelation[0], relation);
                sum += relation;
            }

            for (Map.Entry<String, Integer> movieRelation : map.entrySet()) {
                String outputKey = movieRelation.getKey();
                double normalize = (double)movieRelation.getValue() / (double)sum;
                String outputValue = key.toString() + normalize;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }
}
