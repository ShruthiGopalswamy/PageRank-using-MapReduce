import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AdjacencyGraphMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	   private Text title = new Text();
	   private Text link = new Text();
	
	   @Override
		  protected void map(LongWritable _key, Text _value, Context context)
		      throws IOException, InterruptedException {
		   
			String line = _value.toString();
			String[] parts = line.split("\t", -1);
			title.set(parts[0]);
			link.set("");
			if (parts.length > 1) {
			    link.set(parts[1]);
			}
			context.write(title, link);
	    }
}
