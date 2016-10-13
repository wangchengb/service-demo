package storm.demo.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.demo.bolt.WordCount;
import storm.demo.bolt.WordNormalizer;
import storm.demo.spout.WordReader;

/**
 * Created by chenwang206234 on 2016/10/12.
 */
public class WordCountTopology {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder() ;
        builder.setSpout("word-reader", new WordReader(), 2) ;
        builder.setBolt("word-normalizer", new WordNormalizer(), 3).shuffleGrouping("word-reader") ;
        builder.setBolt("word-counter", new WordCount(), 3).fieldsGrouping("word-normalizer", new Fields("no")) ;

        //config
        Config config = new Config() ;
        config.put("wordFile", "text.txt") ;
        config.setDebug(false);

        //Topology
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1) ;

        if(args.length >= 1){
            String cluster = args[0] ;
            if(cluster.contains("cluster")){
                config.setNumWorkers(2);
                try {
                    StormSubmitter.submitTopology(args[0], config, builder.createTopology());
                } catch (AlreadyAliveException e) {
                    e.printStackTrace();
                } catch (InvalidTopologyException e) {
                    e.printStackTrace();
                } catch (AuthorizationException e) {
                    e.printStackTrace();
                }
            }
        }else {
            //local cluster
            LocalCluster cluster = new LocalCluster() ;
            cluster.submitTopology("Getting-Started-Toplogie", config, builder.createTopology());
            Thread.sleep(5000);
            cluster.shutdown();
        }


    }
}
