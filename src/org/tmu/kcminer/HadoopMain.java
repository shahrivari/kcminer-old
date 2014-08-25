package org.tmu.kcminer;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.tmu.kcminer.hadoop.GraphLayer;
import org.tmu.kcminer.hadoop.LongArrayWritable;
import org.tmu.kcminer.hadoop.OneKlik;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Saeed on 8/24/14.
 */
public class HadoopMain extends Configured implements Tool {
    static private String WORK_DIR;
    static private final int max_par = 5000;
    static Path input_path;
    static boolean verbose = false;
    static int subgraph_size = 3;
    int nReduces = 1;
    static CommandLineParser parser;
    static CommandLine commandLine;
    static HelpFormatter formatter = new HelpFormatter();
    static Options options = new Options();

    @Override
    public int run(String[] strings) throws Exception {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        Configuration conf = getConf();

        if (commandLine.hasOption("i")) {
            input_path = new Path(commandLine.getOptionValue("i"));
        } else {
            System.out.println("An Input directory must be given.");
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(-1);
        }
        if (commandLine.hasOption("nr")) {
            nReduces = Integer.parseInt(commandLine.getOptionValue("nr"));
            if (nReduces < 1) {
                System.out.println("Number of reduce tasks must be greater or equal to 1.");
                System.exit(-1);
            }
        }

        if (commandLine.hasOption("wd")) {
            WORK_DIR = commandLine.getOptionValue("wd");
        } else {
            System.out.println("A working directory must be given.");
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(-1);
        }


        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(WORK_DIR))) {
            if (!commandLine.hasOption("y")) {
                System.out.print("Work directory " + WORK_DIR + " already exists!  remove it first (y/n)?");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                String line = "";
                try {
                    line = br.readLine();
                } catch (IOException ioe) {
                    System.out.println("IO error trying to read from console!");
                    System.exit(1);
                }
                if (!line.toLowerCase().equals("y")) {
                    System.out.printf("You did not typed 'y'. Then I will quit!!!\n");
                    System.exit(1);
                }
                fs.delete(new Path(WORK_DIR), true);
            } else
                fs.delete(new Path(WORK_DIR), true);

        }
        if (!fs.mkdirs(new Path(WORK_DIR))) {
            throw new IOException("Cannot create input directory " + WORK_DIR);
        }

        Job job = new Job(conf, WORK_DIR);
        job.setJarByClass(HadoopMain.class);
        job.setMapperClass(GraphLayer.Map.class);
        job.setReducerClass(GraphLayer.Reduce.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);
        job.getConfiguration().set("working_dir", WORK_DIR);
        job.getConfiguration().set("mapred.output.compress", "true");
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().set("mapred.compress.map.output", "true");
        job.getConfiguration().set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job.getConfiguration().set("mapred.task.timeout", "36000000");
        FileInputFormat.addInputPath(job, input_path);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(WORK_DIR + "/graph"));
        System.out.println("Set Reduce tasks to " + nReduces);
        job.setNumReduceTasks(nReduces);

        job.waitForCompletion(true);
        System.out.printf("Took %s.\n", stopwatch);

        Job job2 = new Job(conf, WORK_DIR);
        job2.setJarByClass(HadoopMain.class);
        job2.setMapperClass(OneKlik.Map.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongArrayWritable.class);
        job2.setNumReduceTasks(0);
        job2.getConfiguration().set("working_dir", WORK_DIR);
        job2.getConfiguration().set("mapred.output.compress", "true");
        job2.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job2.getConfiguration().set("mapred.compress.map.output", "true");
        job2.getConfiguration().set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        job2.getConfiguration().set("mapred.task.timeout", "36000000");
        FileInputFormat.addInputPath(job2, new Path(WORK_DIR + "/graph"));
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(WORK_DIR + "/1"));
        System.out.println("Set Reduce tasks to " + nReduces);
        job2.setNumReduceTasks(nReduces);

        int result = job2.waitForCompletion(true) ? 0 : 1;
        System.out.printf("Took %s.\n", stopwatch);

        return result;

    }

    public static void main(String[] args) throws Exception {
        Stopwatch watch = new Stopwatch().start();
        options.addOption("nr", "nreduce", true, "number of reduce tasks.");
        options.addOption("y", "overwrite", false, "overwrite output if exists.");
        options.addOption("v", "verbose", false, "verbose mode.");
        options.addOption("i", "input", true, "the input graph's file name.");
        options.addOption("wd", true, "the working directory.");

        parser = new BasicParser();
        commandLine = parser.parse(options, args);

        if (commandLine.hasOption("v"))
            verbose = true;

        System.exit(ToolRunner.run(null, new HadoopMain(), args));
    }

}
