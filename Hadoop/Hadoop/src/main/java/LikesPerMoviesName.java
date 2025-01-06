import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class LikesPerMoviesName {

    //JOB 1
    //Extract most rated movies per user
    public static class TopRatedMoviesMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields[0].equals("userId")) {return;}
            int userId = Integer.parseInt(fields[0]);
            String movieId = fields[1];
            double rating = Double.parseDouble(fields[2]);

            //keep only the movies with a rating of 5
            //OUTPUT : userId, movieI
            context.write(new IntWritable(userId), new Text(movieId+"/"+rating));
        }
    }

    //Choose only one film per user
    public static class TopRatedMoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Keep the highest rating per users
            double maxRating = Double.MIN_VALUE;
            String movieIdToWrite = "";
            for (Text movie : values) {
                String[] fields = movie.toString().split("/");
                String movieId = fields[0];
                double rating = Double.parseDouble(fields[1]);

                if (rating > maxRating) {
                    maxRating = rating;
                    movieIdToWrite = movieId;
                    //We can stop the loop if we find a rating of 5 (the maximum)
                    if (maxRating == 5) {break;}
                }
            }
            //OUTPUT : userId, movieId (only one per user -> if we found a 5, we stop the loop // if not, we keep the previous highest rating)
            context.write(key, new Text(movieIdToWrite));
        }
    }

    //JOB 2
    //Add one when a movie appear
    public static class MovieCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String movieId = fields[1];
            //OUTPUT : movieId, 1
            context.write(new Text(movieId), one);
        }
    }

    //Add all the ones per movies
    public static class MovieCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //OUTPUT : movieId, sum
            context.write(key, new IntWritable(sum));
        }
    }

    //Sort the movies by popularity (most likes)
    public static class InvertMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String movieId = fields[0];
            int count = Integer.parseInt(fields[1]);
            //OUTPUT : sum, movieId
            context.write(new IntWritable(count), new Text(movieId));
        }
    }

    //JOB3
    //Add the films with the same number of likes on the same line
    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final HashMap<String, String> movieIdToName = new HashMap<>();
        //The setup will run before the reduce; it will load the movie file in the cache
        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path movieFilePath = new Path(cacheFiles[0]);
                BufferedReader reader = new BufferedReader(new FileReader(movieFilePath.getName()));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains(",\"")) {
                        String filmName = line.split("\"")[1];
                        String movieId = line.split(",")[0];
                        movieIdToName.put(movieId, filmName);
                    } else {
                        String[] classicSplit = line.split(",");
                        String filmName = classicSplit[1];
                        String movieId = classicSplit[0];
                        movieIdToName.put(movieId, filmName);
                    }
                }
                reader.close();
            }
        }

        //The final step, we will write the movies with the same number of likes on the same line
        //OUTPUT : sum, movieName1 movieName2 ...
        //by default, the movies are sorted by the number of likes (ascending)
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder movies = new StringBuilder();
            for (Text val : values) {
                if (movies.length() > 0) {
                    movies.append(" ");
                }
                movies.append(movieIdToName.get(val.toString()));
            }
            context.write(key, new Text(movies.toString()));
        }
    }

    //the main function will run the 3 jobs
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: LikesPerMoviesName <ratings_path> <movies_path> <output_path>");
            System.exit(-1);
        }

        String ratingsPathSTR = args[0];
        Path ratingsPath = new Path(ratingsPathSTR);
        String moviesPathSTR = args[1];
        Path moviesPath = new Path(moviesPathSTR);
        String resultFiles = args[2];
        String finalOutputPath = resultFiles + "/output";
        Path outputPath = new Path(finalOutputPath);

        String intermediate1 = resultFiles + "/intermediate/intermediate1";
        String intermediate2 = resultFiles + "/intermediate/intermediate2";

        Path intermediate1Path = new Path(intermediate1);
        Path intermediate2Path = new Path(intermediate2);


        // Job 1: Extraction of the most rated movies per user
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Top Rated Movies");
        job1.setJarByClass(LikesPerMoviesName.class);
        job1.setMapperClass(TopRatedMoviesMapper.class);
        job1.setReducerClass(TopRatedMoviesReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, ratingsPath);
        //use intermediate path to store the output of the first job
        FileOutputFormat.setOutputPath(job1, intermediate1Path);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Count users per movie
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Count Users Per Movie");
        job2.setJarByClass(LikesPerMoviesName.class);
        job2.setMapperClass(MovieCountMapper.class);
        job2.setReducerClass(MovieCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //use the previous intermediate path as input
        FileInputFormat.addInputPath(job2, intermediate1Path);
        //use the second intermediate path to store the output of the second job
        FileOutputFormat.setOutputPath(job2, intermediate2Path);

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        //Job 3: Sort movies by popularity and output the film names
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Sort Movies by Popularity");
        //Add the movies file to the cache to compute it in the setup
        job3.addCacheFile(moviesPath.toUri());
        job3.setJarByClass(LikesPerMoviesName.class);
        job3.setMapperClass(InvertMapper.class);
        job3.setReducerClass(SortReducer.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);

        //use the previous intermediate path as input
        FileInputFormat.addInputPath(job3, intermediate2Path);
        //use the final output path to store the output of the third job
        FileOutputFormat.setOutputPath(job3, outputPath);

        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }

        System.out.println("Jobs completed successfully.");
    }
}
