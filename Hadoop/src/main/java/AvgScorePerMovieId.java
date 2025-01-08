import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvgScorePerMovieId {
    public static class AvgScorePerMovieIdMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] informations = value.toString().split(",");
            if (informations[0].equals("userId")) {return;}
            String movieId = informations[1];
            double score = Double.parseDouble(informations[2]);
            context.write(new Text(movieId), new DoubleWritable(score));
        }
    }

    public static class AvgScorePerMovieIdReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            double sum = 0;
            for (DoubleWritable value : values) {
                count++;
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: AvgScorePerMovieId <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Starting MapReduce job with input: " + args[0] + ", output: " + args[1]);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "avg_score_per_movie_id");

        job.setJarByClass(AvgScorePerMovieId.class);
        job.setMapperClass(AvgScorePerMovieIdMapper.class);
        job.setReducerClass(AvgScorePerMovieIdReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}