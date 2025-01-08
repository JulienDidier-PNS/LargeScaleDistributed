import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.jute.compiler.JFile;

import java.io.IOException;

public class MoviesCounterFromSpecificGenre {

    public static class MoviesCounterFromSpecificGenreMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //movieId,title,genres
            if (value.toString().contains("\",")) {
                //get the film name (between quotes if , in name) and the genres
                String filmName = value.toString().split("\"")[1];
                String[] otherAndGenre = value.toString().split("\",");
                String genres = otherAndGenre[otherAndGenre.length - 1];
                String[] genresArray = genres.split("\\|");
                for (String genre : genresArray) {context.write(new Text(genre), new Text(filmName));}
            }
            else{
                String[] classicSplit = value.toString().split(",");
                String filmName = classicSplit[1];
                String genres = classicSplit[classicSplit.length - 1];
                String[] genresArray = genres.split("\\|");
                for (String genre : genresArray) {context.write(new Text(genre), new Text(filmName));}
            }
        }
    }

    public static class MoviesCounterFromSpecificGenreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String genre = context.getConfiguration().get("custom.genre");
            for (Text value : values) {
                if (key.toString().equals(genre)) {context.write(key, value);}
            }
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 3) {
            System.err.println("Usage: MoviesCounterFromSpecificGenre <input path> <output path> <genre>");
            System.exit(-1);
        }

        System.out.println("Starting MapReduce job with input: " + args[0] + ", output: " + args[1]);

        Configuration conf = new Configuration();
        conf.set("custom.genre", args[2]);

        Job job = Job.getInstance(conf, "specific_genre");

        job.setJarByClass(MoviesCounterFromSpecificGenre.class);
        job.setMapperClass(MoviesCounterFromSpecificGenreMapper.class);
        job.setReducerClass(MoviesCounterFromSpecificGenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
