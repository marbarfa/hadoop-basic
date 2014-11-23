import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
        //creating a JobConf object and assigning a job name for identification purposes
        Job conf = new Job(getConf(), WordCount.class.getSimpleName());

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        //Providing the mapper and reducer class names
        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);
        
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        boolean executedOk = conf.waitForCompletion(true);

        return executedOk ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new WordCount(),args);
        System.exit(res);
    }
    
    
}