package patos.ao3

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

fun main(args: Array<String>) {
    if (args.size != 2) {
        System.err.println("2 arguments needed: <in> <out>")
        System.exit(2)
    }
    val inputPath = args[0]
    val outputPath = args[1]
    val job = Job.getInstance(Configuration())

    FileInputFormat.setInputPaths(job, Path(inputPath))
    FileOutputFormat.setOutputPath(job, Path(outputPath))
    // Outputs the fanfic's id and it's contents
    job.outputKeyClass = Text::class.java
    job.outputValueClass = Text::class.java

    job.mapOutputKeyClass = Text::class.java
    job.mapOutputValueClass = Text::class.java

    job.mapperClass = FanficContentMapper::class.java
}