import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Movie {

    public static class MovieMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //movieId,title,genres
            if (value.toString().contains("\",")) {
                String[] otherAndGenre = value.toString().split("\",");
                String genres = otherAndGenre[otherAndGenre.length - 1];
                String[] genresArray = genres.split("\\|");
                for (String genre : genresArray) {context.write(new Text(genre), new LongWritable(1));}
            }
            else{
                String[] classicSplit = value.toString().split(",");
                String genres = classicSplit[classicSplit.length - 1];
                String[] genresArray = genres.split("\\|");
                for (String genre : genresArray) {context.write(new Text(genre), new LongWritable(1));}
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {count += value.get();}
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: moviemapper <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Starting MapReduce job with input: " + args[0] + ", output: " + args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(Movie.class);
        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(MovieReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
