import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {

    private final LongWritable result = new LongWritable();
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
	
			throws IOException, InterruptedException {
		// defines the input from the mapper as a long writable, so it can store bigger numbers
		// the total of the mapper imput is divided by the total count to get the average length of tweets	    
		long total = 0;
		long count = 0;
	    for (IntWritable value: values){
	    	total += value.get();
		count += 1;

	    }

	    result.set(total/count);
	    context.write(key,result);
	 

	}
}
