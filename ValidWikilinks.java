import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ValidWikilinks {
	
	private static HashMap<String, String> outPaths;

    public static void initialFiles (String outBucketName) {
    	
        outPaths = new HashMap<String, String>();
        
        String tempPath = outBucketName + "/temp/";
        String resultPath = outBucketName + "/graph/";
        
        outPaths.put("job1Tmp", tempPath);
        outPaths.put("job2Tmp", resultPath);
    }
    
    // Job 1: extract outlink from XML file and remove red link
    public static void parseXml(String input, String output) throws Exception {
        Configuration conf = new Configuration();
    	conf.set(XMLInputFormat.START_TAG_KEY, "<page>");
    	conf.set(XMLInputFormat.END_TAG_KEY, "</page>");	
        Job xmlExtractJob = Job.getInstance(conf, "xmlExtract");
        xmlExtractJob.setJarByClass(ExtractValidLinksMapper.class);
    	xmlExtractJob.setInputFormatClass(XMLInputFormat.class);
    	xmlExtractJob.setOutputFormatClass(TextOutputFormat.class);
        xmlExtractJob.setMapperClass(ExtractValidLinksMapper.class);
        xmlExtractJob.setReducerClass(ExtractValidLinksReducer.class);
        xmlExtractJob.setOutputKeyClass(Text.class);
        xmlExtractJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(xmlExtractJob, new Path(input));
    	FileOutputFormat.setOutputPath(xmlExtractJob, new Path(output));

    	xmlExtractJob.waitForCompletion(true);
    }    
    
    // Job 2: Generate adjacency outlink graph
    public static void getAdjacencyGraph(String input, String output) throws Exception {
    	Configuration conf = new Configuration();
		Job adjacencyGraphJob = Job.getInstance(conf, "adjacencyGraph");
		adjacencyGraphJob.setJarByClass(ValidWikilinks.class);
		adjacencyGraphJob.setMapperClass(AdjacencyGraphMapper.class);
		adjacencyGraphJob.setReducerClass(AdjacencyGraphReducer.class);
		FileInputFormat.addInputPath(adjacencyGraphJob, new Path(input));
		FileOutputFormat.setOutputPath(adjacencyGraphJob, new Path(output));
		adjacencyGraphJob.setOutputKeyClass(Text.class);
		adjacencyGraphJob.setOutputValueClass(Text.class);
        
		adjacencyGraphJob.waitForCompletion(true);
    }
    
public static void main(String[] args) throws Exception {
    	
 		/*Configuration conf = new Configuration();
 		Path outputDir = new Path(args[1]);
 		outputDir.getFileSystem(conf).delete(outputDir, true);	    
 		
 		FileSystem fs = FileSystem.get(new URI(args[1]), conf);
 		ValidWikilinks.initialFiles(args[1]);
 		
 		// extract wiki and remove redlinks
 		System.out.println("ParseXML"); 		
 		ValidWikilinks.parseXml(args[0], outPaths.get("job1Tmp"));
 		
 		// wiki adjacency graph generation
 		System.out.println("Adj Graph"); 		
 		ValidWikilinks.getAdjacencyGraph(outPaths.get("job1Tmp"), outPaths.get("job2Tmp"));*/
	
	Configuration conf = new Configuration();
	Path outputDir = new Path(args[1]);
	outputDir.getFileSystem(conf).delete(outputDir, true);
	FileSystem fs = FileSystem.get(new URI(args[1]), conf);
	ValidWikilinks.initialFiles(args[1]);
	// extract wiki and remove redlinks
	//System.out.println("ParseXML");
	ValidWikilinks.parseXml(args[0], outPaths.get("job1Tmp"));
	//System.out.println("Adj Graph");
	// wiki adjacency graph generation
	ValidWikilinks.getAdjacencyGraph(outPaths.get("job1Tmp"),
			outPaths.get("job2Tmp"));
	
    }
}
