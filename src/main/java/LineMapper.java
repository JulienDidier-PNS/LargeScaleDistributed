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
import java.util.ArrayList;
import java.util.List;

public class LineMapper {
    private static int lineNumber = 0;

    // Mapper Class
    public static class mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("Mapper: Processing key = " + key + ", value = " + value);
            context.write(new LongWritable(lineNumber), value);
            lineNumber++;
        }
    }

    // Reducer Class
    public static class LineReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reducer: Processing key = " + key);
            List<String> lines = new ArrayList<>();
            for (Text value : values) {
                System.out.println("Reducer: Adding value = " + value);
                lines.add(value.toString());
            }
            context.write(key, new Text(lines.toString()));
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: wordcount <input path> <output path>");
            System.exit(-1);
        }

        System.out.println("Starting MapReduce job with input: " + args[0] + ", output: " + args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "linemapper");

        job.setJarByClass(LineMapper.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(LineReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Job configuration completed. Submitting job...");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
