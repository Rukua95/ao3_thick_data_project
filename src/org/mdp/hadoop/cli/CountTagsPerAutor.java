package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Java class to run a remote Hadoop word count job.
 *
 * Contains the main method, an inner Reducer class
 * and an inner Mapper class.
 *
 * @author El mejor grupo de patos
 */
public class CountTagsPerAutor {

    /**
     * Use this with line.split(SPLIT_REGEX) to get fairly nice
     * word splits.
     */
    public static String SPLIT_REGEX = "\t";
    public static String SUBSPLIT_REGEX = "( )*,( )*";
    /**
     * This is the Mapper Class. This sends key-value pairs to different machines
     * based on the key.
     *
     * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
     *
     * InputKey we don't care about (a LongWritable will be passed as the input
     * file offset, but we don't care; we can also set as Object)
     *
     * InputKey will be Text: a line of the file
     *
     * MapKey will be Text: a word from the file
     *
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
     *
     * @author Aidan
     *
     */
    // Probablemente hay errores con el formato de guardado.
    public static class CountTagsPerAutorMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();


        /**
         * Given the offset in bytes of a line (key) and a line (value),
         * we will output (word,1) for each word in the line.
         *
         * @throws InterruptedException
         *
         */
        @Override
        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] rawWords = line.split(SPLIT_REGEX);
            String cat = rawWords[3] + "," +  rawWords[4];
            String[] tags= cat.split(SUBSPLIT_REGEX);

            String year = rawWords[2];

            for(String s1 : tags) {
                String s = year + "###" + s1;
                word.set(s);

                output.write(word, one);
            }
        }
    }

    /**
     * This is the Reducer Class.
     *
     * This collects sets of key-value pairs with the same key on one machine.
     *
     * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
     *
     * MapKey will be Text: a word from the file
     *
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
     *
     * OutputKey will be Text: the same word
     *
     * OutputValue will be IntWritable: the final count
     *
     * @author Aidan
     *
     */
    // ActorsInFilmsReducer
    public static class CountTagsPerAutorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Given a key (a word) and all values (partial counts) for
         * that key produced by the mapper, then we will sum the counts and
         * output (word,sum)
         *
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context output) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            output.write(key, new IntWritable(sum));

        }
    }

    /**
     * Main method that sets up and runs the job
     *
     * @param args First argument is input, second is output
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: "+CountTagsPerAutor.class.getName()+" <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(CountTagsPerAutorMapper.class);
        job.setCombinerClass(CountTagsPerAutorReducer.class); // in this case a combiner is possible!
        job.setReducerClass(CountTagsPerAutorReducer.class);

        job.setJarByClass(CountTagsPerAutor.class);
        job.waitForCompletion(true);
    }
}
