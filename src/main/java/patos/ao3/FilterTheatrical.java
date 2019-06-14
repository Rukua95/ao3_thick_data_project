package patos.ao3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Java class to run a remote Hadoop movieKey count job.
 * <p>
 * Contains the main method, an inner Reducer class and an inner Mapper class.
 *
 * @author Aidan
 */
public class FilterTheatrical {

    /**
     * java.patos.ao3.Main method that sets up and runs the job
     *
     * @param args First argument is input, second is output
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: " + FilterTheatrical.class.getName() + " <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(FilterTheatricalMapper.class);
        //job.setCombinerClass(FilterTheatricalReducer.class); // in this case a combiner is possible!
        job.setReducerClass(FilterTheatricalReducer.class);

        job.setJarByClass(FilterTheatrical.class);
        job.waitForCompletion(true);
    }

    /**
     * This is the Mapper Class. This sends key-value pairs to different machines based on the key.
     * <p>
     * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
     * <p>
     * InputKey we don't care about (a LongWritable will be passed as the input file offset, but we
     * don't care; we can also set as Object)
     * <p>
     * InputKey will be Text: a line of the file
     * <p>
     * MapKey will be Text: a movieKey from the file
     * <p>
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the movieKey
     *
     * @author Aidan
     */
    public static class FilterTheatricalMapper extends Mapper<Object, Text, Text, Text> {

        private final Text movieKey = new Text();
        private final Text movieVal = new Text();


        /**
         * Given the offset in bytes of a line (key) and a line (value), we will output (movieKey,1) for
         * each movieKey in the line.
         */
        @Override
        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {
            String line = value.toString();
            String SPLIT_REGEX = "\t";
            String[] parsedLine = line.split(SPLIT_REGEX);
            String outKey = parsedLine[3] + "##" + parsedLine[4] + "##" + parsedLine[5];
            String outVal = parsedLine[1] + "##" + parsedLine[2] + "##" + parsedLine[6];
            movieKey.set(outKey.toLowerCase());
            movieVal.set(outVal.toLowerCase());
            output.write(movieKey, movieVal);
        }
    }

    /**
     * This is the Reducer Class.
     * <p>
     * This collects sets of key-value pairs with the same key on one machine.
     * <p>
     * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
     * <p>
     * MapKey will be Text: a movieKey from the file
     * <p>
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the movieKey
     * <p>
     * OutputKey will be Text: the same movieKey
     * <p>
     * OutputValue will be IntWritable: the final count
     *
     * @author Aidan
     */
    public static class FilterTheatricalReducer extends
            Reducer<Text, Text, Text, Text> {

        /**
         * Given a key (a movieKey) and all values (partial counts) for that key produced by the mapper,
         * then we will sum the counts and output (movieKey,sum)
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context output)
                throws IOException, InterruptedException {
            String type;
            for (Text value : values) {
                type = value.toString().split("##")[2];
                if (type.equalsIgnoreCase("THEATRICAL_MOVIE"))
                    output.write(key, value);
            }
        }
    }
}
