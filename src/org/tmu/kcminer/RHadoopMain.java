package org.tmu.kcminer;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.tmu.kcminer.hadoop.GraphLayer;
import org.tmu.kcminer.hadoop.LongArrayWritable;
import org.tmu.kcminer.hadoop.RandomLongPartitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Saeed on 8/24/14.
 */
public class RHadoopMain extends Configured implements Tool {
    static private String WORK_DIR;
    static String input_path;
    static boolean verbose = false;
    int nMaps = 1;
    static CommandLineParser parser;
    static CommandLine commandLine;
    static HelpFormatter formatter = new HelpFormatter();
    static Options options = new Options();
    static int cliqueSize = 3;

    @Override
    public int run(String[] strings) throws Exception {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        if (commandLine.hasOption("s")) {
            cliqueSize = Integer.parseInt(commandLine.getOptionValue("s"));
            if (cliqueSize < 3) {
                System.out.println("Size of clique must be greater or equal to 3.");
                System.exit(-1);
            }
        } else {
            System.out.println("Size of subgraphs must be given.");
            formatter.printHelp(Main.class.toString(), options);
            System.exit(-1);
        }

        if (commandLine.hasOption("i")) {
            input_path = commandLine.getOptionValue("i");
        } else {
            System.out.println("An Input directory must be given.");
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(-1);
        }
        if (commandLine.hasOption("nm")) {
            nMaps = Integer.parseInt(commandLine.getOptionValue("nr"));
            if (nMaps < 1) {
                System.out.println("Number of map tasks must be greater or equal to 1.");
                System.exit(-1);
            }
        } else {
            System.out.println("Number of map tasks must be given.");
            System.exit(-1);
        }

        if (commandLine.hasOption("wd")) {
            WORK_DIR = commandLine.getOptionValue("wd");
        } else {
            System.out.println("A working directory must be given.");
            formatter.printHelp(this.getClass().toString(), options);
            System.exit(-1);
        }


        final FileSystem fs = FileSystem.get(getConf());
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

        Graph graph = Graph.buildFromEdgeListFile(input_path);
        List<List<Long>> chunks = new ArrayList<List<Long>>(nMaps);
        for (int i = 0; i < nMaps; i++)
            chunks.add(new ArrayList<Long>());

        int list_i = 0;
        for (long l : graph.vertices) {
            chunks.get(list_i).add(l);
            list_i = (list_i + 1) % nMaps;
        }

        System.out.print("Writing input for Map #:");
        for (int i = 0; i < chunks.size(); ++i) {
            final Path file = new Path("", "part" + i);
            final SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, getConf(), file,
                    LongWritable.class, NullWritable.class);
            try {
                for (Long l : chunks.get(i))
                    writer.append(new LongWritable(l), NullWritable.get());
            } finally {
                writer.close();
            }
            if (i % 50 != 0)
                System.out.print(".");
            else
                System.out.print(i);
        }
        System.out.println(nMaps + ".");


        Job job = new Job(getConf(), "GraphInit");
        job.setJarByClass(RHadoopMain.class);
        job.setMapperClass(GraphLayer.Map.class);
        job.setReducerClass(GraphLayer.Reduce.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongArrayWritable.class);
        job.setPartitionerClass(RandomLongPartitioner.class);
        job.getConfiguration().set("working_dir", WORK_DIR);
        job.getConfiguration().set("mapred.output.compress", "true");
        job.getConfiguration().set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        job.getConfiguration().set("mapred.compress.map.output", "true");
        job.getConfiguration().set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        job.getConfiguration().set("mapred.task.timeout", "36000000");
        job.getConfiguration().set("mapred.max.split.size", "524288");
        //FileInputFormat.addInputPath(job, input_path);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(WORK_DIR + "/graph"));
        System.out.println("Set Reduce tasks to " + nMaps);
        job.setNumReduceTasks(nMaps);

        job.waitForCompletion(true);
        System.out.printf("Took %s.\n", stopwatch);
        return 1;

    }

    public static void main(String[] args) throws Exception {
        KlikState state = new KlikState(2, new long[]{1, 2, 3, 4, 5});
        long[] arr = state.toLongs();
        KlikState ss = KlikState.fromLongs(arr);

        Stopwatch watch = new Stopwatch().start();
        options.addOption("nm", "nmap", true, "number of map tasks.");
        options.addOption("y", "overwrite", false, "overwrite output if exists.");
        options.addOption("v", "verbose", false, "verbose mode.");
        options.addOption("i", "input", true, "the input graph's file name.");
        options.addOption("wd", true, "the working directory.");
        options.addOption("s", "size", true, "maximum size of clique to enumerate.");

        parser = new BasicParser();
        commandLine = parser.parse(options, args);

        if (commandLine.hasOption("v"))
            verbose = true;

        System.exit(ToolRunner.run(null, new RHadoopMain(), args));
    }

}
