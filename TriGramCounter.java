import java.io.IOException;
import java.util.ArrayList;
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
            ArrayList<String> words = value.toString().replaceAll("[^a-zA-Z ]", "").split("\\s+"); // remove punctuation and split on the setOutputValueClass

            // iterate through the list until 3 from end and create 3-grams
            for (int i = 0; i < words.size() - 2; i++){
                ArrayList<String> threeGram = new ArrayList<String>(words.subList(i, i+3)); // create a list of 3-grams
                String threeGramString = String.join(" ", threeGram); // convert list to string, separated by spaces
                word.set(threeGramString); // set the word to be a 3-gram
                context.write(word, one); // assign value 'one' to word
            }

            /*
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'"); // separates line into list of words based on criteria

            // iterate through each word in the tokenized list
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken()); // set the word
                context.write(word, one); // assign value 'one' to word
            }*/
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // define where the input is specified - from CL
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // define where the output should go - from CL
        System.exit(job.waitForCompletion(true) ? 0 : 1); // finish up the job
    }
}
