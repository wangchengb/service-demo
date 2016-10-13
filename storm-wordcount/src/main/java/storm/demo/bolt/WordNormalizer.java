package storm.demo.bolt;

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

/**
 * Created by chenwang206234 on 2016/10/12.
 */
public class WordNormalizer implements IRichBolt {

    private OutputCollector collector ;
    private String name ;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector ;
        this.name = topologyContext.getStormId() ;
    }

    public void execute(Tuple tuple) {
        //System.out.println("name:"+name);
        int index = tuple.getInteger(0) ;
        String sentence = tuple.getString(1) ;
        String[] words = sentence.split(" ") ;
        for(String word : words){
            List a = new ArrayList() ;
            a.add(index) ;
            a.add(word);

            this.collector.emit(a) ;
        }
        collector.ack(tuple);
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
