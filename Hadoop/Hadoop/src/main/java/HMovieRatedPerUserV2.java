import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class HMovieRatedPerUserV2 extends Configured implements Tool {
    public static class HMovieRatedPerUserMapperV2 extends Mapper<Object, Text, Text, Text> {
        private final HashMap<String, String> movieIdToName = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles(); //Get filepath from distributed cache
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
                        if (classicSplit.length >= 2) {
                            String movieId = classicSplit[0];
                            String filmName = classicSplit[1];
                            movieIdToName.put(movieId, filmName);
                        }
                    }
                }
                reader.close();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] informations = value.toString().split(",");
            if (informations[0].equals("userId")) {return;}
            String movieId = informations[1];
            String score = informations[2];
            String userId = informations[0];
            if (movieIdToName.containsKey(movieId)) {
                context.write(new Text(userId), new Text(movieIdToName.get(movieId) + "," + score));
            } else {
                context.write(new Text(userId), new Text("\""+movieId+"\"" + "," + score));
            }
        }
    }

    public static class HMovieRatedPerUserReducerV2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double highestScore = Double.MIN_VALUE;
            String toKeep = "";
            for (Text keyVal : values) {
                String[] informations = keyVal.toString().split(",");
                String scoreSTR = informations[informations.length - 1];
                if (scoreSTR != null) {
                    double score = Double.parseDouble(scoreSTR);
                    if (score > highestScore) {
                        highestScore = score;
                        toKeep = keyVal.toString();
                    }
                }
            }
            context.write(new Text(key), new Text(toKeep));
        }
    }
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        // checking the arguments count
        if (args.length != 3) {
            System.err.println("Usage : HMovieRatedPerUserV2 <input1> <movieName_Input> <output>");
            System.exit(0);
        }
        int res = ToolRunner.run(new Configuration(), new HMovieRatedPerUserV2(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String source1 = args[0]; // ratings.csv
        String source2 = args[1]; // movies.csv
        String dest = args[2];    // output path

        Configuration conf = new Configuration();
        conf.set("custom.moviefile", source2);

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "Movie Rated Per User with Movie Names");

        job.setJarByClass(HMovieRatedPerUserV2.class);
        job.setMapperClass(HMovieRatedPerUserMapperV2.class);
        job.setReducerClass(HMovieRatedPerUserReducerV2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(source2).toUri());

        FileInputFormat.addInputPath(job, new Path(source1));
        TextOutputFormat.setOutputPath(job, new Path(dest));

        Path outPath = new Path(dest);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // Lancer le job
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
