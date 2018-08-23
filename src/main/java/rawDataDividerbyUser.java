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

    public static class rawDataDividerReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            for (Text value : values) {
                stringBuilder.append("," + value.toString());
            }
            //key = user value=movie1:rating1,movie2:rating2...
            context.write(key, new Text(stringBuilder.toString().replaceFirst(",", "")));
        }
    }
}
