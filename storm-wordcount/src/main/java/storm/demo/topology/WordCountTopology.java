package storm.demo.topology;

import org.apache.log4j.Logger;
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

    private static final Logger logger = Logger.getLogger(WordCountTopology.class);

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder() ;
        builder.setSpout("word-reader", new WordReader(), 2) ;
        builder.setBolt("word-normalizer", new WordNormalizer(), 3).shuffleGrouping("word-reader") ;
        builder.setBolt("word-counter", new WordCount(), 3).setNumTasks(5).fieldsGrouping("word-normalizer", new Fields("no")).fieldsGrouping("word-reader", new Fields("no")) ;

        // config
        Config config = new Config() ;
        config.put("wordFile", "text.txt") ;
        config.setDebug(false);

        // Topology
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1) ;

        if(args.length >= 1){
            String cluster = args[0] ;
            if(cluster.contains("cluster")){
                config.setNumWorkers(2);
                try {
                    StormSubmitter.submitTopology(args[0], config, builder.createTopology());
                } catch (AlreadyAliveException e) {
                    logger.error("", e);
                } catch (InvalidTopologyException e) {
                    logger.error("", e);
                } catch (AuthorizationException e) {
                    logger.error("", e);
                }
            }
        }else {
            //local cluster
            LocalCluster cluster = new LocalCluster() ;
            cluster.submitTopology("Getting-Started-Toplogie", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }


    }
}
