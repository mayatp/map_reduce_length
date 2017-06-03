import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 
    private IntWritable one = new IntWritable(1);
    private IntWritable tweet_len = new IntWritable();
    
    // Sample input
    // epoch_time [0];tweetId [1];tweet(hashtag contained)[2]; device [3]
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        //1: Find feature and partial value
        
        String[] line = value.toString().split(";");
	// checks if the line actually contains a tweet then it checks if the twitter is longer than 140 (default maximum input for tweets)
        if (line.length == 4) {
        	int tweet = line[2].length();
		if (tweet <= 140) {
		tweet_len.set(tweet);
		}
		
        }
        context.write(one,tweet_len);
    }
}
