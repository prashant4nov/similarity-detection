import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class SimilarityReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (Text value : values) {
			int val = 0;
			try{
				 val += Integer.parseInt(value.toString());	
			}
			catch(Exception e){
				val = 1;
			}
			sum += val;
		}
		context.write(key, new Text(sum+""));
	}
	
	

}