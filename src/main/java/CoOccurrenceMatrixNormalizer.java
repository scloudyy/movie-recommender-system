import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CoOccurrenceMatrixNormalizer {

    public static class MatrixNomalizerMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // inputKey: movie1:movie2
            // inputValue: sum
            // outputKey: movie1
            // outputValue: movie2:sum
            String[] moviesRelation = value.toString().trim().split("\t");
            String[] movies = moviesRelation[0].trim().split(":");

            context.write(new Text(movies[0]), new Text(movies[1] + ":" + moviesRelation[1]));
        }
    }

}
