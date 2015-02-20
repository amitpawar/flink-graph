package flink.graphs;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.*;

import flink.graphs.example.GCExample;


public class TestGraphColouring {

	private Map<Integer, Integer> colorMap;
	
	private String inputFilePath = "/home/amit/impro/colortest3";
	private String outputFolderPath = "/home/amit/impro/testColor3OP";
	private String maxIterations = "10"; 
	
	
	//Tests no two neighbor has same color
	@Test
	public void testNeighborColors() throws Exception{
		String[] args = {inputFilePath, outputFolderPath, maxIterations};
		GCExample.main(args);
		File outputDirectory = new File(outputFolderPath);
		
		colorMap = new HashMap();
		String line;
		int i = 0;
        for (File fileEntry : outputDirectory.listFiles()) {
        	if(fileEntry.isDirectory()){
        		if(fileEntry.getName().contains("colour"))
        		{
        			i++;
        			for(File insideFile: fileEntry.listFiles())
        			{
        				BufferedReader br = new BufferedReader(new FileReader(insideFile));
			            while ((line = br.readLine()) != null) {
			               int vertexId = Integer.parseInt(line.substring(0, 1));	
			               colorMap.put(vertexId, i);
			            }
			            br.close();
        			}
        		}
        	}
        }
        
        
        File inputFile = new File(inputFilePath);
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String edge;
        Pattern SEPARATOR = Pattern.compile("[ \t,]");
        while((edge = reader.readLine())!= null){
        	
        	String[] tokens = SEPARATOR.split(edge);

        	int source = Integer.parseInt(tokens[0]);
			int target = Integer.parseInt(tokens[1]);
			
			junit.framework.TestCase.assertFalse(colorMap.get(source) == colorMap.get(target));
        }
        
	}
}
