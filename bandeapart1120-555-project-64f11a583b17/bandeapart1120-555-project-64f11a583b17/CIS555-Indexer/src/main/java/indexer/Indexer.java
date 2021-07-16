package indexer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import storage.DocS3Block;
import storage.DocS3Controller;
import storage.DocS3Entity;

public class Indexer {
	static HashSet<String> stops = new HashSet<String>();
	static HashMap<String, Double> counts = null;
	static HashMap<String, Double> finalOutputs = null;
	
	// LongWrittable - Document Id
	// Text - Document
	// Text - Word 
	// IntWrittable - number of times term is seen
	public static class WordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = "";
			if (value != null) {
				line = value.toString().replaceAll("\\p{Punct}", "");
			}
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			StringTokenizer s = new StringTokenizer(line);
			Text curr = new Text();
			DoubleWritable one = new DoubleWritable(1);
			
			while(s.hasMoreTokens()) {
				String curTok = s.nextToken().toLowerCase();
				if (!stops.contains(curTok) && curTok.matches("^[a-zA-Z0-9]*$")) {
					curr.set(curTok + "\t" + fileName);
					context.write(curr, one);
				}
			}
		}
	}
	
	
	public static class WordCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		@Override
		public void reduce(Text key, Iterable <DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<DoubleWritable> vals = values.iterator();
			String[] wordFile = key.toString().split("\t");
			String fileName = wordFile[1];
			Text out = new Text();
			while(vals.hasNext()) {
				sum ++; 	
				vals.next();
			}	
			double tf = sum/ counts.get(fileName);
			finalOutputs.put(key.toString(), tf);
		}
	} 
	
	public static class DocWordCountMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString().replaceAll("\\p{Punct}", "");
			
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			StringTokenizer s = new StringTokenizer(line);
			String curTok;
			Text curr = new Text(fileName);
			double count = 0;
			while(s.hasMoreTokens()) {
				curTok = s.nextToken().toLowerCase();
				if (!stops.contains(curTok) && curTok.matches("^[a-zA-Z0-9]*$")) {
					count++;
				}
			}
			context.write(curr, new DoubleWritable(count));
		}
	}
	
	public static class DocWordCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable <DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			Iterator<DoubleWritable> vals = values.iterator();
			while(vals.hasNext()) {
				sum += vals.next().get();
			}	
			counts.put(key.toString(), sum);		
		}
	} 
	
	static private boolean runJob0(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf, "docWordCount");
		job.setJarByClass(Indexer.class);
		job.setMapperClass(DocWordCountMapper.class);
		job.setReducerClass(DocWordCountReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean status = job.waitForCompletion(true);
		return status;
	} 
	
	static private boolean runJob1(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(Indexer.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setNumReduceTasks(10);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean status = job.waitForCompletion(true);
		return status;
	} 
	
	static public void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		// connect to database
		// pull all files I want to index
		
		
		String[] stopWordsArray = { "a", "about", "above", "across",
		        "after", "afterwards", "again", "against", "all", "almost", "alone",
		        "along", "already", "also", "although", "always", "am", "among",
		        "amongst", "amoungst", "amount", "an", "and", "another", "any",
		        "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around",
		        "as", "at", "back", "be", "became", "because", "become", "becomes",
		        "becoming", "been", "before", "beforehand", "behind", "being", "below",
		        "beside", "besides", "between", "beyond", "bill", "both", "bottom",
		        "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con",
		        "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
		        "down", "due", "during", "each", "eg", "eight", "either", "eleven",
		        "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every",
		        "everyone", "everything", "everywhere", "except", "few", "fifteen",
		        "fify", "fill", "find", "fire", "first", "five", "for", "former",
		        "formerly", "forty", "found", "four", "from", "front", "full",
		        "further", "get", "give", "go", "had", "has", "hasnt", "have", "he",
		        "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon",
		        "hers", "herse", "him", "himse", "his", "how", "however", "hundred",
		        "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it",
		        "its", "itse", "keep", "last", "latter", "latterly", "least", "less",
		        "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
		        "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
		        "my", "myse", "name", "namely", "neither", "never", "nevertheless",
		        "next", "nine", "no", "nobody", "none", "noone", "nor", "not",
		        "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
		        "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
		        "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
		        "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
		        "seems", "serious", "several", "she", "should", "show", "side", "since",
		        "sincere", "six", "sixty", "so", "some", "somehow", "someone",
		        "something", "sometime", "sometimes", "somewhere", "still", "such",
		        "system", "take", "ten", "than", "that", "the", "their", "them",
		        "themselves", "then", "thence", "there", "thereafter", "thereby",
		        "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
		        "third", "this", "those", "though", "three", "through", "throughout",
		        "thru", "thus", "to", "together", "too", "top", "toward", "towards",
		        "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
		        "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
		        "whence", "whenever", "where", "whereafter", "whereas", "whereby",
		        "wherein", "whereupon", "wherever", "whether", "which", "while",
		        "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
		        "with", "within", "without", "would", "yet", "you", "your", "yours",
		        "yourself", "yourselves"};
		
		for (String stop: stopWordsArray) {	
			stops.add(stop);
		}
		counts = new HashMap<String, Double>();
		finalOutputs = new HashMap<String, Double>();
		String storageDirectory = args[0];
		File inputs = new File(storageDirectory + "/inputs");
		File wordCounts = new File(storageDirectory + "/wordCounts");
		File outputs = new File(storageDirectory + "/outputs");
		ArrayList<String> currBlocks = new ArrayList<String>();
		int i = 0;
		List<String> blockNames = DocS3Controller.listFilesInS3();
		try (DocS3Controller docS3Controller = new DocS3Controller()) {
			
			for (String blockName: blockNames) {
				try {
					if (WordIndexController.blockIndexed(blockName) > 0) {
						
						continue;
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					WordIndexController.addBlock(blockName);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				DocS3Block docS3Block = docS3Controller.getEntireDocBlock(blockName);
				Iterator<DocS3Entity> it = docS3Block.iterator();
				//System.out.println(docS3Block.getEntityCount());
				while (it.hasNext()) {
					DocS3Entity entity = it.next();
					inputs.mkdir();
					File f = new File(inputs.toString() + "/" + DocS3Entity.toHexString(entity.getDocId()));
					f.createNewFile();
					try (FileOutputStream outputStream = new FileOutputStream(f)) {
						Document document = Jsoup.parse(new String (entity.getContentBytes()));
						Elements textItems = document.getElementsByTag("p");
						textItems.addAll(document.getElementsByTag("h1"));
						textItems.addAll(document.getElementsByTag("h2"));
						textItems.addAll(document.getElementsByTag("h3"));
						textItems.addAll(document.getElementsByTag("h4"));
						textItems.addAll(document.getElementsByTag("h5"));
						textItems.addAll(document.getElementsByTag("h6"));
					    for (Element t : textItems) {
					    	outputStream.write((t.text() + "\n").getBytes());
					    }								
					}
				}
				try {
					WordIndexController.addBlock(blockName);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				currBlocks.add(blockName);
				i++;
				if (i == 1) {
					boolean status0 = runJob0(inputs.toString(), wordCounts.toString());
					FileUtils.deleteDirectory(wordCounts);
					if (!status0) { 
						System.out.println("something wrong");
						FileUtils.deleteDirectory(outputs);
				        FileUtils.cleanDirectory(inputs);
						System.exit(1);
					}
					boolean status1 = runJob1(inputs.toString(), outputs.toString());	

					
					
					if (!status1) { 
						System.out.println("something wrong");
						System.exit(1);
					}

					try {
						WordIndexController.addWord(finalOutputs);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					FileUtils.deleteDirectory(outputs);
			        FileUtils.cleanDirectory(inputs);
					counts = new HashMap<String, Double>();
					finalOutputs = new HashMap<String, Double>();
					i = 0;
					currBlocks.clear();
				}
				System.exit(0);
			}
		}
	}
}
