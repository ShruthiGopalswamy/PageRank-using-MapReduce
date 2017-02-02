import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AdjacencyGraphReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
		StringBuilder sb = new StringBuilder();
		for (Text value : values) {
		    String val = value.toString();
		    if (!val.equals("")) {
		    	sb.append(val + '\t');
		    }
		}
		if (sb.length() > 0) {
		    sb.setLength(sb.length() - 1);
		    context.write(key, new Text(sb.toString()));
		} else {
			context.write(key, null);
		}
    }
}
