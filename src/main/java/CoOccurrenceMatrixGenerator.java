import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CoOccurrenceMatrixGenerator {
    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: line number
            // inputValue: user \t movie1:rating1,movie2:rating2...
            // outputKey: movie1:movie2
            // outputValue: 1
            String[] userMovieRating = value.toString().trim().split("\t");
            String[] movieRating = userMovieRating[1].trim().split(",");

            for (int i = 0; i < movieRating.length; i++) {
                String movie1 = movieRating[i].trim().split(":")[0];

                for (int j = 0; j < movieRating.length; j++) {
                    String movie2 = movieRating[j].trim().split(":")[0];
                    context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                }
            }
        }
    }
}
