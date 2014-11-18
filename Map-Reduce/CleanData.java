import java.io.IOException;
import java.util.*;
import java.io.*;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class CleanData {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    List<List<String>> gtfs_tuples; 
    double sigma = 100.0; //meters

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      /* GTFS fields: linha,descricao,agencia,sequencia,latitude,longitude */
      System.out.println("Setup called");

      gtfs_tuples = new ArrayList<List<String>>();

      try (BufferedReader br = new BufferedReader(new FileReader("/Users/alibezz/Documents/nyu_courses/mda/project/data/gtfs_todas-linhas-paradas.csv"))) {
	String sCurrentLine;
	while ((sCurrentLine = br.readLine()) != null) {
          gtfs_tuples.add(Arrays.asList(sCurrentLine.split(",")));
	}
 
      } catch (IOException e) {
	e.printStackTrace();
      }

      System.out.println(gtfs_tuples.size()); 
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

      return dist;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split(","));
        double lat = Double.parseDouble(fields.get(9));
        double lng = Double.parseDouble(fields.get(10));

        boolean less_than_sigma = false;
        double distance = 0.;
        for(List<String> gtfs_entry: gtfs_tuples) {
          double gtfs_lat = Double.parseDouble(gtfs_entry.get(4));
          double gtfs_lng = Double.parseDouble(gtfs_entry.get(5));
          distance = distFrom(lat, lng, gtfs_lat, gtfs_lng);
          if(distance <= sigma) {
            less_than_sigma = true;
            break;
          }
        }

        if(less_than_sigma) {
          word.set(line);
          context.write(word, one); //this value is useful to eliminate replications
          //System.out.println("Point is less than sigma meters away from any gtfs ntry: " + distance);
        }
    }
 } 
       
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
 
      int sum = 0;
      for(IntWritable val : values) {
         sum += val.get();
      }

      context.write(key, new IntWritable(sum));
    }
 }
    
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "cleandata");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(CleanData.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
