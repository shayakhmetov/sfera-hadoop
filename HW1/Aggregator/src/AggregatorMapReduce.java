import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class AggregatorMapReduce {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, OutputCollector<Int, IntWritale> output, Reporter reporter) throws IOException{
            if(key != 0) {
                String[] splitted = value.toString().split(',');
                output.collect(Text(splitted[1]), Text(splitted[4]));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private HashMap<String, Integer> countries = new HashMap<String, Integer>();
        private ArrayList<Integer> values = new ArrayList<Integer>();
        public void reduce(Text key, Iterator<Text> iterator,
                           OutputCollector<Text, IntWritable, IntWritable, DoubleWritable, IntWritable, DoubleWritable, DoubleWritable> output,
                           Reporter reporter) throws IOException{
            String year = key.toString();
            while(iterator.hasNext()){
                String country = iterator.toString();
                if(countries.get(country) != null){
                    countries.put(country, countries.get(country) + 1);
                }
                else{
                    countries.put(country, 1);
                }
            }
            for(Integer value: countries.values()){
                values.add(value);
            }
            output.collect(Text(year), values.size(), get_minimum(values), get_median(values), get_maximum(values), get_average(values), get_dispersion(values));

        }

        private Integer get_minimum(ArrayList<Integer> values){
            return Collections.min(values);
        }

        private Double get_median(ArrayList<Integer> values){
            Collections.sort(values);
            if(values.size() % 2 != 0){
                return (double) values.get(values.size()/2);
            }
            else{
                return (double)(values.get(values.size()/2) + values.get(values.size()/2 - 1)) / 2.0;
            }
        }

        private Integer get_maximum(ArrayList<Integer> values){
            return Collections.max(values);
        }

        private Double get_average(ArrayList<Integer> values){
            double sum = 0;
            for(Integer value: values){
                sum += value;
            }
            return sum / values.size();
        }

        private Double get_dispersion(ArrayList<Integer> values){
            double average = get_average(values);
            double sum = 0;
            for(Integer value: values){
                sum += Math.pow(value - average, 2);
            }
            return Math.sqrt(sum / values.size());
        }
    }

    public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf(AggregatorMapReduce.class);
        conf.setJobName("Aggregator");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValuesClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPaths(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
