import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ExtractValidLinksMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final String START_TAG_TITLE = "<title>";
	private static final String END_TAG_TITLE = "</title>";
	private static final String START_TAG_TEXT = "<text";
	private static final String END_TAG_TEXT = "</text>";
	private static final String CHAR_SEPARATOR = "|";
	private static final String CHAR_SPACE = " ";
	private static final String CHAR_UNDERSCORE = "_";
	
	  protected void map(LongWritable _key, Text _value, Context context)
	      throws IOException, InterruptedException {
    	
    	String page = _value.toString();
    	String text = ""; 
    	String title = ""; 
    	    	
    	title = page.substring(page.indexOf(START_TAG_TITLE) + START_TAG_TITLE.length(), page.indexOf(END_TAG_TITLE)).replace(CHAR_SPACE, CHAR_UNDERSCORE);

    	if(page.indexOf(START_TAG_TEXT) != -1 && page.indexOf(END_TAG_TEXT) != -1) 
    		text = page.substring(page.indexOf(START_TAG_TEXT), page.indexOf(END_TAG_TEXT));
    	
    	Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]"); 
    	Matcher matcher = pattern.matcher(text);
    	
    	//This part handles the red links 
    	context.write(new Text(title), new Text("#")); 

    	while(matcher.find()){
    		String tempLink = matcher.group(1);
    		
    		if(tempLink != null && !tempLink.isEmpty()){
    			if(tempLink.contains(CHAR_SEPARATOR)){ 
    				String outlink = tempLink.substring(0, tempLink.indexOf(CHAR_SEPARATOR)).replace(CHAR_SPACE, CHAR_UNDERSCORE); 
    				if (!outlink.equals(title)) 
    					context.write(new Text(outlink), new Text(title)); 
    			} else { 
    				String outlink = tempLink.replace(CHAR_SPACE, CHAR_UNDERSCORE); 
    				if (!outlink.equals(title)) 
    					context.write(new Text(outlink), new Text(title)); 
    			  }     			
    		}
    	}
    }

}
