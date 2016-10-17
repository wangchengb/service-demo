package storm.demo.spout;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by chenwang206234 on 2016/10/12.
 */
public class WordReader implements IRichSpout{

    private static final Logger logger = Logger.getLogger(WordReader.class);

    private SpoutOutputCollector collector ;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("no", "line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector ;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        doSth() ;
        Utils.sleep(10000);
    }

    public void ack(Object o) {
        //System.out.println("OK:" + o);
    }

    public void fail(Object o) {
        //System.out.println("FAIL:" + o);
    }

    public void doSth(){
        String[] strs = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"} ;
        Random rand = new Random() ;

        for(int i=0; i<1000; i++){
            int r = rand.nextInt(strs.length) ;
            Values v = new Values() ;
            v.add(r) ;
            v.add(strs[r]) ;
            this.collector.emit(v, r+"") ;
        }
    }

}
