package org.tmu.kcminer;

import org.apache.commons.cli.*;
import org.tmu.kcminer.hadoop.HadoopMain;
import org.tmu.kcminer.hadoop.ReplicatedHadoopMain;
import org.tmu.kcminer.smp.IntGraph;
import org.tmu.kcminer.smp.IntKlikState;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Saeed on 8/1/14.
 */
public class Main {
    static CommandLineParser parser = new BasicParser();
    static Options options = new Options();
    static HelpFormatter formatter;
    static CommandLine commandLine;
    static String input_path;
    static String output_path = null;
    static boolean verbose = false;
    static int cliqueSize = 3;
    static int lowerBound = 3;
    static int threadCount = Runtime.getRuntime().availableProcessors();
    static Stopwatch stopwatch = new Stopwatch();


    private static void initCLI(String[] args) throws ParseException {
        options.addOption("i", "input", true, "the input file name.");
        options.addOption("s", "size", true, "maximum size of clique to enumerate.");
        options.addOption("o", "output", true, "the output file name (default out.txt)");
        options.addOption("e", "enumerate", false, "enumerate cliques.");
        options.addOption("c", "count", false, "just count.");
        options.addOption("local", false, "run in local mode.");
        options.addOption("max", false, "just maximals and upper size cliques.");
        options.addOption("l", "lower", true, "lower size for clique (default k).");
        options.addOption("t", "threads", true, "number of threads to use.");
        options.addOption("i32", false, "use 32-bit nodes.");
        options.addOption("v", "verbose", false, "suppress progress report.");
        formatter = new HelpFormatter();
        commandLine = parser.parse(options, args);
    }

    private static void localMain() throws IOException, InterruptedException {
        stopwatch.reset().start();
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

        if (commandLine.hasOption("l")) {
            lowerBound = Integer.parseInt(commandLine.getOptionValue("l"));
            if (lowerBound < 3) {
                System.out.println("Size of clique must be greater or equal to 3.");
                System.exit(-1);
            }
        } else
            lowerBound = cliqueSize;


        if (commandLine.hasOption("i")) {
            input_path = commandLine.getOptionValue("i");
        } else {
            System.out.println("Input file must be given.");
            formatter.printHelp(Main.class.toString(), options);
            System.exit(-1);
        }

        if (commandLine.hasOption("o"))
            output_path = commandLine.getOptionValue("o");

        if (commandLine.hasOption("t"))
            threadCount = Integer.parseInt(commandLine.getOptionValue("t"));
        System.out.println("Running locally.");

        if (commandLine.hasOption("i32")) {//use 32-bit
            System.out.println("Using 32bit ids.");
            IntGraph graph = new IntGraph();
            graph.buildFromEdgeListFile(input_path);
            System.out.printf("Graph loaded in %s.\n", stopwatch.toString());
            System.out.println(graph.getInfo());
            stopwatch.reset().start();
            if (commandLine.hasOption("max")) {
                System.out.printf("Maximal Cliques are not supported in 32bit ids.\n");
                System.exit(1);
            }
            if (commandLine.hasOption("c") || commandLine.hasOption("e")) {
                if (commandLine.hasOption("c"))
                    output_path = null;
                else if (output_path == null && commandLine.hasOption("e")) {
                    System.out.println("An output file must be given.");
                    System.exit(-1);
                }
                long count = IntKlikState.parallelEnumerate(graph, lowerBound, cliqueSize, threadCount, output_path);
                System.out.printf("Cliques of size %d to %d: %,d\n", lowerBound, cliqueSize, count);
                System.out.printf("Took in %s.\n", stopwatch.toString());
                System.exit(0);
            } else {
                System.out.println("In 32bit id mode, just fixed size cliques can be handled!");
                System.exit(-1);
            }
        } else {
            System.out.println("Using 64bit ids.");
            Graph graph = Graph.buildFromEdgeListFile(input_path);
            System.out.printf("Graph loaded in %s.\n", stopwatch.toString());
            System.out.println(graph.getInfo());
            stopwatch.reset().start();
            if (commandLine.hasOption("c") || commandLine.hasOption("e")) {
                if (commandLine.hasOption("c"))
                    output_path = null;
                else if (output_path == null) {
                    System.out.println("An output file must be given.");
                    System.exit(-1);
                }
                if (commandLine.hasOption("max")) {
                    long count = KlikState.parallelEnumerate(graph, lowerBound, cliqueSize, threadCount, true, output_path);
                    System.out.printf("Maximal Cliques of size %d to %d: %,d\n", lowerBound, cliqueSize, count);
                } else {
                    long count = KlikState.parallelEnumerate(graph, lowerBound, cliqueSize, threadCount, false, output_path);
                    System.out.printf("Cliques of size %d to %d: %,d\n", lowerBound, cliqueSize, count);
                }
            } else {
                System.out.println("No option is provided!");
                System.exit(-1);
            }
            System.out.printf("Took %s.\n", stopwatch.toString());
            System.exit(0);
        }

        System.exit(0);
    }

    public static void main(String[] args) throws Exception {
        if (Arrays.asList(args).contains("-local")) {
            initCLI(args);
            localMain();
        } else if (Arrays.asList(args).contains("-rhadoop"))
            ReplicatedHadoopMain.main(args);
        else if (Arrays.asList(args).contains("-hadoop"))
            HadoopMain.main(args);
        else {
            System.out.println("Just runs locally now.");
            formatter.printHelp(Main.class.toString(), options);
            System.exit(-1);
        }
    }
}
