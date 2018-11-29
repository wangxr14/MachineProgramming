package team.cs425.g54.topology;

import java.util.ArrayList;

public class Topology {
    ArrayList<Record> recordList;
    public ArrayList<Spout> spoutList;
    public ArrayList<Bolt>  boltList;
    public void addRecode(Record record){
        recordList.add(record);
    }
    public ArrayList<Record> getRecordList(){
        return (ArrayList<Record>) recordList.clone();
    }
    public void addSpout(String functionType,String file){
        Spout spout = new Spout(file,functionType);
        spoutList.add(spout);
    }
    public void addBolt(String functiontype,String info){
        Bolt bolt = new Bolt(functiontype,info);
        boltList.add(bolt);
    }
}
