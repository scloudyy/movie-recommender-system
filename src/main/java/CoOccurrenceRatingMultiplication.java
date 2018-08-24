import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorDescriptor;

import javax.xml.bind.ValidationEvent;
import java.io.IOException;

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
}
