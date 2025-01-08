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
import java.util.HashMap;

public class HMovieRatedPerUser {
    public static class HMovieRatedPerUserMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] informations = value.toString().split(",");
            if (informations[0].equals("userId")) {return;}
            String score = informations[2];
            String movieId = informations[1];
            String userId = informations[0];
            context.write(new Text(movieId), new Text(userId+","+movieId+","+score));
        }
    }

    public static class HMovieRatedPerUserReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double highestScore = Double.MIN_VALUE;
            String toKeep = "";
            for(Text keyVal : values){
                String[] informations = keyVal.toString().split(",");
                String userId = informations[0];
                String movieId = informations[1];
                String scoreSTR = informations[2];
                if (scoreSTR != null) {
                    double score = Double.parseDouble(scoreSTR);
                    if (score > highestScore) {
                        highestScore = score;
                        toKeep = userId;
                    }
                }
            }
            context.write(new Text(key), new Text(toKeep));
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: HMovieRatedPerUser <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Starting MapReduce job with input: " + args[0] + ", output: " + args[1]);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "avg_score_per_movie_id");

        job.setJarByClass(HMovieRatedPerUser.class);
        job.setMapperClass(HMovieRatedPerUserMapper.class);
        job.setReducerClass(HMovieRatedPerUserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
