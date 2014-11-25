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
        
public class AverageDistance {
        
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    HashMap<String, ArrayList<String>> busStopsPerLine = new HashMap<String, ArrayList<String>>(); 

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
      /* GTFS fields: linha,descricao,agencia,sequencia,latitude,longitude */

      try {
        URL url = new URL("https://s3.amazonaws.com/rio-cleaned-data/gtfs_todas-linhas-paradas.csv");
        Scanner s = new Scanner(url.openStream());
        while (s.hasNextLine()) {
          String line = s.nextLine();
          List<String> fields = Arrays.asList(line.split(","));
          String point = fields.get(4) + ",";
          point += fields.get(5);
          if(busStopsPerLine.containsKey(fields.get(0))) {
            busStopsPerLine.get(fields.get(0)).add(point);
          } else {
            ArrayList<String> pointsList = new ArrayList<String>();
            pointsList.add(point);
            busStopsPerLine.put(fields.get(0), pointsList);
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

      return dist;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /* Data fields: ID,NomePonto,Linha,Onibus,Hora,DiaMes,Mes,Ano,Velocidade,LatitudePonto,LongitudePonto,TimeStamp,DescricaoPonto */
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split(","));
        double lat = Double.parseDouble(fields.get(9));
        double lng = Double.parseDouble(fields.get(10));

        ArrayList<String> points = busStopsPerLine.get(fields.get(2)); 
        double minDistance = Double.POSITIVE_INFINITY;
        if(points == null) {
          //System.out.println("Line " + fields.get(2) + " not found");
          word.set("line in entry " + fields.get(2) + " not found in bus stops.");
          context.write(word, new DoubleWritable(minDistance));
        } else {
          //The loop below finds the bus stop in the same line
          //of the gps entry in question that is the closest
          for(String busStop: points) {
            List<String> coords = Arrays.asList(busStop.split(","));
            double gtfsLat = Double.parseDouble(coords.get(0));
            double gtfsLng = Double.parseDouble(coords.get(1));
            double distance = distFrom(lat, lng, gtfsLat, gtfsLng);
            if(distance < minDistance) {
              minDistance = distance;
            }
          }
          if(minDistance == Double.POSITIVE_INFINITY) {
            System.out.println("Still infinity.");
          }
          word.set(fields.get(2));
          context.write(word, new DoubleWritable(minDistance));
        }
    }
 } 
       
 public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {

      //computes average distance from gps entries to their closest bus stops 
      double num = 0., den = 0.;
      for(DoubleWritable val : values) {
         num += val.get();
         den += 1.;
      }
      
      context.write(key, new DoubleWritable(num/den));
    }
 }
    
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "averagedistance");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(AverageDistance.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
