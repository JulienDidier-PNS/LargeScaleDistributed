import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class HMovieNameRatedPerUser {
    public static class HMovieNameRatedPerUserMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] informations = value.toString().split(",");
            if (informations[0].equals("userId")) {return;}
            String movieId = informations[1];
            String score = informations[2];
            String userId = informations[0];
            context.write(new Text(userId), new Text(movieId + "," + score));
        }
    }

    public static class HMovieNameRatedPerUserReducer extends Reducer<Text, Text, Text, Text> {
        private final HashMap<String, String> movieIdToName = new HashMap<>();
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
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double highestScore = Double.MIN_VALUE;
            String movieToKeep = "";
            for(Text keyVal : values){
                String[] informations = keyVal.toString().split(",");
                String scoreSTR = informations[1];
                String movieId = informations[0];
                if (scoreSTR != null) {
                    double score = Double.parseDouble(scoreSTR);
                    if (score > highestScore) {
                        highestScore = score;
                        movieToKeep = movieId;
                    }
                }
            }
            context.write(new Text(key), new Text(movieIdToName.get(movieToKeep)));
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length != 3) {
            System.err.println("Usage: HMovieNameRatedPerUser <ratings> <movies> <output path>");
            System.exit(-1);
        }

        String ratingsPath = args[0];
        String moviesPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "HMovieNameRatedPerUser");
        job.addCacheFile(new Path(moviesPath).toUri());

        job.setJarByClass(HMovieRatedPerUser.class);
        job.setMapperClass(HMovieNameRatedPerUserMapper.class);
        job.setReducerClass(HMovieNameRatedPerUserReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(ratingsPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
