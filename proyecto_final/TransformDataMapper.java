import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class TransformDataMapper extends Mapper<LongWritable, Text, Text, NullWritable>
{
    //hadoop supported data types
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    //map method that performs the tokenizer job and framing the initial key value pairs
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the input string into a nice map
        Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
        // Grab the necessary XML attributes
        String user_id = parsed.get("UserId");
        String text = parsed.get("Text");
        String row_id = parsed.get("Id");
        String postId = parsed.get("PostId");
        if (row_id != null && user_id!=null){
            context.write(new Text("\"" + row_id + "\";\"" + user_id + "\";\"" + text + "\";\"" + postId + "\""),
                    NullWritable.get());
        }
        // xml --> csv (job en mapreduce para transformaci'on de datos)
        // csv --Load --> hive
        // grails app --- query --> hive (1 string)

    }
}

