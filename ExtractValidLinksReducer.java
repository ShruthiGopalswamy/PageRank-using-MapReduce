import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtractValidLinksReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {		
		HashSet<String> pages = new HashSet<>();
		for (Text value : values) {
		    pages.add(value.toString());
		}
		if (pages.contains("#")) {
			context.write(null , key);
		    pages.remove("#");
		    pages.remove(key.toString());
		    for (String p : pages) {
		    	context.write(new Text(p), new Text(key + "\t"));
		    }
		}
    }	
}
