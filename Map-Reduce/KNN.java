 import java.io.IOException;
 import java.util.*;
 import java.io.*;        
 import java.net.URL;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.conf.*;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.*;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 public class KNN {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		 private Text word = new Text();

		 //Sorts entries that co-occur day-wise with any entry without Linha
		 //according to its Latitude
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				 /* aggregates entries by a key that corresponds to the entry's date */
				 /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
				 String line = value.toString();
				 List<String> fields = Arrays.asList(line.split(","));

				 if(fields.get(2).length() > 1) { //This entry has Linha, so it can be useful
					 String busKey = fields.get(5);
					 busKey += fields.get(6);
					 busKey += fields.get(7);
					 //If uncommented, lines below will add hour to the key
					 //key += "-";
					 //key += fields.get(4);

					 String busValue = fields.get(2);
					 busValue += ",";
					 busValue += fields.get(9);
					 busValue += ",";
					 busValue += fields.get(10);

					 word.set(busKey);
					 context.write(word, new Text(value));
				 }
		 }
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		 int K = 30; //TODO Make K context-dependent -- i.e. if 10 points are full of entropy, improve it
		 HashMap<String, ArrayList<String>> busEntriesPerDateTime = new HashMap<String, ArrayList<String>>(); 

		 protected void setup(Reducer.Context context) throws IOException, InterruptedException {

			 /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
			 try {
				 URL url = new URL("https://s3-us-west-2.amazonaws.com/mapreduceemptylines/entries_without_lines.csv");
				 Scanner s = new Scanner(url.openStream());
				 while (s.hasNextLine()) {
					 String line = s.nextLine();
					 List<String> fields = Arrays.asList(line.split(","));

					 //this entry has empty Linha
					 /* Key format: DiaMes,Mes,Ano */ 
					 String key = fields.get(5);
					 key += fields.get(6);
					 key += fields.get(7);
					 //If uncommented, lines below will add hour to the key
					 //key += "-";
					 //key += fields.get(4);

					 if(busEntriesPerDateTime.containsKey(key)) {
						 busEntriesPerDateTime.get(key).add(line);
					 } else {
						 ArrayList<String> valuesList = new ArrayList<String>();
						 valuesList.add(line);
						 busEntriesPerDateTime.put(key, valuesList);
					 }
				 }
				 s.close();
			 } catch (IOException e) {
	 e.printStackTrace();
			 }
		 }

		 private static double distFrom(double lat1, double lng1, double lat2, double lng2) {
			 double earthRadius = 6371; //kilometers
			 double dLat = Math.toRadians(lat2-lat1);
			 double dLng = Math.toRadians(lng2-lng1);
			 double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
									Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
									Math.sin(dLng/2) * Math.sin(dLng/2);
			 double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
			 double dist =  earthRadius * c;
			 //System.out.println(lat1 + " " + lng1 + " " + lat2 + " " + lng2 + " -- " + dist);

			 return dist;
		 }


		 public void reduce(Text reducerKey, Iterable<Text> values, Context context) 
			 throws IOException, InterruptedException {


				 //copy values, as we are going to iterate over them more than once
				 ArrayList<String> cacheValues = new ArrayList<String>();
				 for(Text v: values) {
						 cacheValues.add(v.toString());
				 }

				 //gets K closest points in same day-month-year and assigns most common line
				 //to a point without line
					 ArrayList<String> pointsWithoutLine = busEntriesPerDateTime.get(reducerKey.toString());
					 System.out.println("number of points without line for this reducerkey: " + pointsWithoutLine.size());
						for(String p: pointsWithoutLine) {

							 // Gets K closest points to p (in values)
							 String[] pfields = p.split(",");
							 PriorityQueue<Double> pq = new PriorityQueue<Double>();
							 HashMap<Double, String> pointDistMap = new HashMap<Double, String>(); //assumes no two points have the same distance TODO check if this is reasonable
							 for(String v: cacheValues) {
									 String[] vfields = v.split(",");
									 double dist = distFrom(Double.parseDouble(pfields[9]), Double.parseDouble(pfields[10]), Double.parseDouble(vfields[9]), Double.parseDouble(vfields[10]));     
									 pq.add(dist);
									 pointDistMap.put(dist, v.toString());
							 }

							int z = 0;
							HashMap<String, Integer> lineFrequencies = new HashMap<String, Integer>();
							while(pq.size() != 0 && z < K) {
								double d = pq.remove();
								String dkey = pointDistMap.get(d).split(",")[2];
								if(lineFrequencies.containsKey(dkey)) {
										lineFrequencies.put(dkey, lineFrequencies.get(dkey) + 1);
								} else {
										lineFrequencies.put(dkey, 1);
								}	
								z++;
							}

							// assigns most frequent line from the K closest points to p
							int maxFreq = -1;
							String maxLine = "";
							for(java.util.Map.Entry<String, Integer> e: lineFrequencies.entrySet()) {
									if(e.getValue() > maxFreq) {
											maxFreq = e.getValue();
											maxLine = e.getKey();
									}
							} 

							String newBusLine = ","; newBusLine += maxLine; newBusLine += ",";
							p = p.replaceAll(",,", newBusLine);
							//System.out.println(p);             
							context.write(new Text(p), new Text(""));
					 } 
		 }
	}

	public static void main(String[] args) throws Exception {
		 Configuration conf = new Configuration();

		 Job job = new Job(conf, "averagedistance");

		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);

		 job.setMapperClass(Map.class);
		 job.setReducerClass(Reduce.class);

		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(TextOutputFormat.class);

		 job.setNumReduceTasks(1);
		 job.setJarByClass(KNN.class);

		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));

		 job.waitForCompletion(true);
	}

 }
