import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class VocalesConsonantesMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    //hadoop supported data types
    private final static Text vocal = new Text("vocal");
    private final static IntWritable uno = new IntWritable(1);
    private final static Character[] vocales = {'a','e','i','o','u'};
    private final static Text consonante = new Text("consonante");

    //map method that performs the tokenizer job and framing the initial key value pairs
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //taking one line at a time and tokenizing the same
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        //iterating through all the words available in that line and forming the key value pair
        while (tokenizer.hasMoreTokens())
        {
            String word = tokenizer.nextToken();
            for(int i=0; i<word.length(); i++){
                for (Character c : vocales){
                    if (c.equals(word.charAt(i))){
                        context.write(vocal, uno);
                    }else{
                        context.write(consonante, uno);
                    }
                }
            }
        }
    }
}

