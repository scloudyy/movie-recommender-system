import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CoOccurrenceRatingMultiplication {
    public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: line number
            // inputValue: movie2 \t movie1:normalizeValue
            // outputKey: movie2
            // outputValue: movie1:normalizeValue
            String[] line = value.toString().trim().split("\t");
            String outputKey = line[0];
            String outputValue = line[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: line number
            // inputValue: user:movie:rating
            // outputKey: movie
            // outputValue: user=rating
            String[] userMovieRating = value.toString().trim().split("\t");
            String user = userMovieRating[0];
            String movie = userMovieRating[1];
            String rating = userMovieRating[2];
            context.write(new Text(movie), new Text(user + "=" + rating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // inputKey: movieA
            // inputValue: <movieB:relation, user=rating, ...>
            // outputKey: user:movieB
            // outputValue: relation * rating
            Map<String, Double> relationMap = new HashMap<>();
            Map<String, Double> ratingMap = new HashMap<>();

            for (Text value : values) {
                String str = value.toString();
                if (str.contains(":")) {
                    String[] movieRelation = str.trim().split(":");
                    relationMap.put(movieRelation[0], Double.valueOf(movieRelation[1]));
                }
                else {
                    String[] userRating = str.trim().split("=");
                    ratingMap.put(userRating[0], Double.valueOf(userRating[1]));
                }
            }

            for (Map.Entry<String, Double> movieRelation : relationMap.entrySet()) {
                String movie = movieRelation.getKey();
                double relation = movieRelation.getValue();

                for (Map.Entry<String, Double> userRating : ratingMap.entrySet()) {
                    String user = userRating.getKey();
                    double rating = userRating.getValue();

                    context.write(new Text(user + ":" + movie), new DoubleWritable(relation * rating));
                }
            }
        }
    }
}
