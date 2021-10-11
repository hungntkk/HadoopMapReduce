package com.hadoop.hazelcastsource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClusterName("dev");
            clientConfig.getNetworkConfig().addAddress("10.1.6.110:5701", "10.1.6.110:5702", "10.1.6.110:5703");

            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

            IMap<String, HazelcastJsonValue> wordMap = client.getMap("data_crawl"); //creates the map proxy
            ObjectMapper mapper = new ObjectMapper();

            Predicate wordPredicate = null;
            List<String> listWord = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                listWord.add(itr.nextToken());
            }
            if (listWord.size()>1){
                Predicate wordPredicate1 = Predicates.equal("word", listWord.get(0));
                Predicate wordPredicate2 = Predicates.equal("word", listWord.get(1));
                wordPredicate = Predicates.or( wordPredicate1 , wordPredicate2);
                for (int i=0; i<listWord.size(); i++){
                    if (i>1){
                        Predicate wordPredicate_i = Predicates.equal("word", listWord.get(1));
                        wordPredicate = Predicates.or( wordPredicate , wordPredicate_i);
                    }
                }
                Predicate predicate = null;
                PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
                Predicate activePredicate = e.is( "active" );
                predicate = Predicates.and( wordPredicate , activePredicate);
                Collection<HazelcastJsonValue> records = wordMap.values( predicate );
                System.out.println("Data querry duoc: ");

                for (HazelcastJsonValue wordObj : records) {
                    Word word_i = mapper.readValue(wordObj.toString(), Word.class);
                    String word1 = word_i.getWord();
                    word.set(word1);
                    context.write(word, one);
                }
            }else if(listWord.size()==1){
                wordPredicate = Predicates.equal("word", listWord.get(0));
                PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
                Predicate activePredicate = e.is( "active" );
                Predicate predicate = Predicates.and( wordPredicate , activePredicate);
                Collection<HazelcastJsonValue> records = wordMap.values( predicate );
                System.out.println("Data querry duoc: ");
                for (HazelcastJsonValue wordObj : records) {
                    Word word_i = mapper.readValue(wordObj.toString(), Word.class);
                    String word1 = word_i.getWord();
                    word.set(word1);
                    context.write(word, one);
                }
            }else{
                try {
                    throw new Exception("Ban muon tim tu nao? ");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Collection<HazelcastJsonValue> getValuesWithIsActive(IMap<String, HazelcastJsonValue> iMap, String isActiveName) {
        PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.is( isActiveName );
        return iMap.values( predicate );
    }
}