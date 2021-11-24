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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriGramCounter {
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
    public static class TGCPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String word = key.toString();
            if(numReduceTasks==0){
                return 0;
            }
            
            // swtich based on first character - send to corresponding reducer
            switch (word.charAt(0)){
                case 'a':
                    return 0;
                break;
                case 'b':
                    return 1;
                break;
                case 'c':
                    return 2;
                break;
                case 'd':
                    return 3;
                break;
                case 'e':
                    return 4;
                break;
                case 'f':
                    return 5;
                break;
                case 'g':
                    return 6;
                break;
                case 'h':
                    return 7;
                break;
                case 'i':
                    return 8;
                break;
                case 'j':
                    return 9;
                break;
                case 'k':
                    return 10;
                break;
                case 'l':
                    return 11;
                break;
                case 'm':
                    return 12;
                break;
                case 'n':
                    return 13;
                break;
                case 'o':
                    return 14;
                break;
                case 'p':
                    return 15;
                break;
                case 'q':
                    return 16;
                break;
                case 'r':
                    return 17;
                break;
                case 's':
                    return 18;
                break;
                case 't':
                    return 19;
                break;
                case 'u':
                    return 20;
                break;
                case 'v':
                    return 21;
                break;
                case 'w':
                    return 22;
                break;
                case 'x':
                    return 23;
                break;
                case 'y':
                    return 24;
                break;
                case 'z':
                    return 25;
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
        job.setNumReduceTasks(26); // set num reducers to 26 - one for each letter in alphabet
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // define where the input is specified - from CL
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // define where the output should go - from CL
        System.exit(job.waitForCompletion(true) ? 0 : 1); // finish up the job
    }
}

