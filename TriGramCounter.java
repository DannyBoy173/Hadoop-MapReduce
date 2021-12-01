import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class TriGramCounter {
    // input: gs://coc105-gutenburg-10000books
    // output: gs://danbaulk-tgc/output

    // creating a custom combiner
    public class TGCCombiner extends CombineTextInputFormat{
    public TGCCombiner()
    {
    // setting block size to 128mb
    this.setMaxSplitSize(134217728L);
    }
    }

    public static class TGCMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1); // defines the number 1 to be counted
        private Text word = new Text();
        
        // mapper function
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase(); // create string and convert to lower case & trim whitespace
            String[] words = line.replaceAll("[^a-z\\s]", "").trim().split("\\s+"); // remove punctuation, trim whitespace and split on whitespace
            ArrayList<String> wordsList = new ArrayList<String>(Arrays.asList(words)); // converts to arraylist - for operations
            System.out.println("The words list: " + wordsList);

            // iterate through the list until 2 from end and create 3-grams
            for (int i = 0; i < wordsList.size() - 2; i++){
                ArrayList<String> triGram = new ArrayList<String>(wordsList.subList(i, i+3)); // create a list of 3-grams
                String triGramString = String.join(" ", triGram); // convert list to string, separated by spaces
                System.out.println("The tri gram string: " + triGramString);

                word.set(triGramString); // set the word to be a 3-gram
                context.write(word, one); // assign value 'one' to word
            }
        }
    }

    // custom partitioner to sort output alphabetically
    public static class TGCPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String word = key.toString();
            if(numReduceTasks==0){
                return 0;
            }
            
            // swtich based on first character - send to corresponding reducer (multiple letters grouped in reducers)
            // this has been load balanced based off observations of frequency of first chars
            switch (word.charAt(0)){
                case 'a':
                    return 0;
                case 'b':
                case 'c':
                    return 1;
                case 'd':
                case 'e':
                case 'f':
                    return 2;
                case 'g':
                case 'h':
                case 'i':
                    return 3;
                case 'j':
                case 'k':
                case 'l':
                case 'm':
                case 'n':
                    return 4;
                case 'o':
                case 'p':
                    return 5;
                case 'q':
                case 'r':
                case 's':
                    return 6;
                case 't':
                    return 7;
                case 'u':
                case 'v':
                case 'w':
                case 'x':
                case 'y':
                case 'z':
                    return 8;
                default:
                    return 0;
            }
        }
    }

    public static class TGCReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        
        // reducer function
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // for each key, aggregate the values (just adds all the ones together)
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum); // set result to be sum
            context.write(key, result); // output: key, result
        }
    }

    public static void main(String[] args) throws Exception {
        // preparing environment for hadoop job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tri gram count");

        job.setJarByClass(TriGramCounter.class); // implementation of job
        job.setMapperClass(TGCMapper.class); // mapper class
        job.setReducerClass(TGCReducer.class); // reducer class
        job.setPartitionerClass(TGCPartitioner.class); // partitioner class
        job.setNumReduceTasks(9); // set num reducers to 9 - grouped by letter frequencies
        job.setInputFormatClass(Converger.class); // set the file input format to be combined and with a split size of 128mb
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // define where the input is specified - from CL
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // define where the output should go - from CL
        
        // turn on speculative execution
        job.setMapSpeculativeExecution(true);
        job.setReduceSpeculativeExecution(true);

        // setup intermediate compression
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        System.exit(job.waitForCompletion(true) ? 0 : 1); // finish up the job
    }
}

