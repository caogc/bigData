

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WhoToFollow {

    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, Entity> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            Integer user = new Integer(Integer.parseInt(st.nextToken()));
            ArrayList<Integer> friends = new ArrayList<>();
            while (st.hasMoreTokens()) {
                Integer friend = Integer.parseInt(st.nextToken());          
                friends.add(friend); 
            }
            
            for (int i=0;i<friends.size();i++) {
                Integer friend =friends.get(i);
                Entity e = new Entity();
                e.setFollowed(new Text(String.valueOf(-friend)));
                context.write(new IntWritable(user), e);
                
                Entity e2 = new Entity();
                e2.setFollowed(new Text(String.valueOf(user)));
                String follows = "";
                for(int j=0;j<friends.size();j++)
                {
                    if(i==j)
                        continue;
                    
                    Integer f =friends.get(j);
                    follows +=f+",";
                }
                
                if(!follows.equals(""))
                    e2.setFollows(new Text(follows.substring(0, follows.length()-1)));
                
                context.write(new IntWritable(friend), e2);
            }
        }
    }

    public static class CountReducer extends Reducer<IntWritable, Entity, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Entity> values, Context context)
                throws IOException, InterruptedException {
            
            List<Integer> followsList = new ArrayList<>();
            Map<Integer,List<String>> followedMap = new HashMap<>();
            while(values.iterator().hasNext())
            {
                Entity e =values.iterator().next();
                int followed = Integer.parseInt(e.getFollowed().toString());
                if(followed<0)
                {
                    followsList.add(-followed);
                    continue;
                }
                
                List<String> follows = new ArrayList<>();
                if(e.getFollows()!=null)
                {
                    follows = Arrays.asList(e.getFollows().toString().split(","));
                }
                followedMap.put(followed, follows);
            }
            
            Map<Integer,Integer> resultMap = new HashMap<>();
            for (Map.Entry<Integer, List<String>> e : followedMap.entrySet()) {
                if(followsList.indexOf(e.getKey())>=0)
                    continue;
                
                int count = 0;
                for(String s : e.getValue())
                {
                    if(followsList.contains(Integer.parseInt(s)))
                    {
                        count++;
                    }
                }
                
                resultMap.put(e.getKey(), count);
            }
            
            resultMap = sortByValue(resultMap);
            
            String s = "";
            for (Map.Entry<Integer, Integer> e : resultMap.entrySet()) {
                s+=e.getKey()+"("+e.getValue()+") ";
            }
            context.write(key,new Text(s));
        }
        
        public Map sortByValue(Map unsortedMap) {
            TreeMap<Integer, Integer> sorted = new TreeMap<Integer, Integer>(new ValueComparator(unsortedMap));
            sorted.putAll(unsortedMap);
            return sorted;
        }
        

         class ValueComparator implements Comparator<Integer> {
        
            private Map<Integer, Integer> map;
        
            public ValueComparator(Map<Integer, Integer> map) {
                this.map = map;
            }
        
            public int compare(Integer a, Integer b) {
                return b-a;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "people you may know");
      job.setJarByClass(WhoToFollow.class);
      job.setMapperClass(AllPairsMapper.class);
      job.setReducerClass(CountReducer.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Entity.class);
      
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class Entity implements WritableComparable<Entity> { 
    
    public Entity()
    {
        this.followed = new Text();
        this.follows = new Text();
    }
    private Text followed;  
    private Text follows; 
    
    public Text getFollowed() {
        return followed;
    }

    public void setFollowed(Text followed) {
        this.followed = followed;
    }

    public Text getFollows() {
        return follows;
    }

    public void setFollows(Text follows) {
        this.follows = follows;
    }

    @Override
    public int compareTo(Entity o) {
        return 0;
    }

    @Override  
    public void write(DataOutput out) throws IOException {  
        if(this.followed!=null)
            this.followed.write(out);  
        if(this.follows!=null)
            this.follows.write(out);  
    }  
  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        if(this.followed!=null)
            this.followed.readFields(in); 
        if(this.follows!=null)
            this.follows.readFields(in);  
    }  
  
}  
