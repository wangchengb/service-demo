package storm.demo.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by chenwang206234 on 2016/10/12.
 */
public class WordCount implements IRichBolt{

    private static final Logger logger = LogManager.getLogger(WordCount.class);

    private String name ;
    private int id ;
    private OutputCollector collector ;
    private Map<String, Integer> counter ;

    private AtomicLong count = new AtomicLong() ;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector ;
        this.name = topologyContext.getThisComponentId() ;
        this.id = topologyContext.getThisTaskId() ;

        this.counter = new HashMap<String, Integer>() ;
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(1) ;

        if(str.contains("30") && counter.get(str) != null && counter.get(str) > 0){
            //collector.fail(tuple);
        }else{
            if(!counter.containsKey(str)){
                counter.put(str, 1) ;
            }else{
                counter.put(str, counter.get(str) + 1) ;
            }
            collector.ack(tuple);
        }
        if(count.incrementAndGet() % 10 == 0){
            printInfo();
        }
    }

    public void cleanup() {
        counter.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void printInfo(){
        logger.info("------------------------------------------------------------");
        logger.info("-- Word Counter [" + name + "-" + id + "] -- count : "+count);
        for (Map.Entry<String, Integer> entry : counter.entrySet()) {
            logger.info("cleanup: "+entry.getKey() + " -> " + entry.getValue());
        }
        logger.info("------------------------------------------------------------");
    }

}
