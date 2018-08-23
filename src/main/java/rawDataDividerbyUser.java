import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class rawDataDividerbyUser {
    public static class rawDataDividerMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userMovieRating = value.toString().trim().split(":");
            String user = userMovieRating[0];
            String movie = userMovieRating[1];
            String rating = userMovieRating[2];

            context.write(new Text(user), new Text(movie + ":" + rating));
        }
    }
}
