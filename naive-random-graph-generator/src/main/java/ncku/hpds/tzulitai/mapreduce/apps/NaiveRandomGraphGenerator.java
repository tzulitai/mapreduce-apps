package ncku.hpds.tzulitai.mapreduce.apps;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NaiveRandomGraphGenerator {
	
	static private final String TMP_DIR_PREFIX = NaiveRandomGraphGenerator.class.getSimpleName();
	
	public static class EdgeGeneratorMapper extends
		Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
		
		public void map(IntWritable sourceNode,
						IntWritable offsetDestNode,
						Context context) throws IOException, InterruptedException {
			
			final Configuration conf = context.getConfiguration();
			final int numOfNodes = Integer.valueOf(conf.get("NUM_OF_NODES"));
			final Double thresholdP = Double.valueOf(conf.get("EDGE_PROB"));
					
			for(int nodeItr=1; nodeItr <= numOfNodes; nodeItr++){
				
				if(nodeItr == sourceNode.get()) continue;
				
				Double makeEdgeP = Math.random();
				if (makeEdgeP >= thresholdP){
					if(sourceNode.get() < nodeItr){
						context.write(sourceNode, new IntWritable(nodeItr));
					} else {
						context.write(new IntWritable(nodeItr), sourceNode);
					}
				}
			}
		}
	}
	
	public static class EdgeValidationReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		public void reduce(IntWritable sourceNode,
						   Iterable<IntWritable> neighborNodes,
						   Context context) throws IOException, InterruptedException {
			
			int lastNode = 0;
			for(IntWritable neighborNode : neighborNodes) {
				if(neighborNode.get() != lastNode){
					lastNode = neighborNode.get();
					context.write(sourceNode, neighborNode);
				} else {
					continue;
				}
			}
		}
	}
	
	public static void generateInitialInput(Path tmpPath, Configuration conf) throws IOException {
		
		final FileSystem fs = FileSystem.get(conf);
		if (fs.exists(tmpPath)) {
			throw new IOException("Tmp directory " + fs.makeQualified(tmpPath)
			+ " already exists. Please remove it first.");
		}
		
		int nodeCount = Integer.valueOf(conf.get("NUM_OF_NODES"));
		int numMaps = nodeCount / 1000;
		
		try {
			for(int partItr = 1; partItr <= numMaps; partItr++) {
				final Path partFile = new Path(tmpPath, "part"+partItr);
				@SuppressWarnings("deprecation")
				final SequenceFile.Writer writer = 
						SequenceFile.createWriter(fs, conf, partFile,
								IntWritable.class, IntWritable.class, CompressionType.NONE);
				
				for(int nodeItr = (int) (partItr*Math.pow(1000, partItr-1)); 
					nodeItr <= partItr*Math.pow(1000, partItr)-1; 
					nodeItr++) {
					
					final IntWritable nodeId = new IntWritable(nodeItr);
					final IntWritable offset = new IntWritable(0);
					
					try {
						writer.append(nodeId, offset);
					} finally {
						writer.close();
					}
				}
			}
		} finally {
			System.out.println("Generated initial file");
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		if (otherArgs.length != ) {
			System.err.println("Usage: NaiveRandomGraphGenerator <num_of_nodes> <probability> <output_path>");
			System.exit(2);
		}
		
		conf.setInt("NUM_OF_NODES", Integer.valueOf(otherArgs[0]));
		conf.setDouble("EDGE_PROB", Double.valueOf(otherArgs[1]));
		
		Job job = new Job(conf, "Naive random connected graph generation");
		
		job.setJarByClass(NaiveRandomGraphGenerator.class);
		job.setMapperClass(EdgeGeneratorMapper.class);
		job.setReducerClass(EdgeValidationReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		long now = System.currentTimeMillis();
		int rand = new Random().nextInt(Integer.MAX_VALUE);
		final Path tmpPath = new Path(TMP_DIR_PREFIX + "_" + now + "_" + rand);
		
		generateInitialInput(tmpPath, conf);
		
		FileInputFormat.setInputPaths(job, tmpPath);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
