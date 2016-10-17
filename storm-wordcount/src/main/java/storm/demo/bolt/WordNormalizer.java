package storm.demo.bolt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by chenwang206234 on 2016/10/12.
 */
public class WordNormalizer implements IRichBolt {

    private static final Logger logger = LogManager.getLogger(WordNormalizer.class);

    private OutputCollector collector ;
    private String name ;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector ;
        this.name = topologyContext.getStormId() ;
    }

    public void execute(Tuple tuple) {
        int index = tuple.getInteger(0) ;
        String sentence = tuple.getString(1) ;
        String[] words = sentence.split(" ") ;
        for(String word : words){
            Values a = new Values() ;
            a.add(index) ;
            a.add(word);

            if(index == 0 && new Random().nextBoolean() && new Random().nextBoolean()) {
                this.collector.fail(tuple);
                //logger.error(tuple);
            }else{
                this.collector.emit(tuple, a);
                collector.ack(tuple);
            }

        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("no", "word"));
        //outputFieldsDeclarer.declareStream(name, new Fields("no", "word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
