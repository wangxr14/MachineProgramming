package team.cs425.g54.topology;

import java.util.ArrayList;

public class Topology {
    public ArrayList<Record> recordList;
    public ArrayList<Spout> spoutList;
    public ArrayList<Bolt>  boltList;
    public Topology(){
        spoutList = new ArrayList<>();
        boltList = new ArrayList<>();
        recordList = new ArrayList<>();
    }
    public void addRecord(Record record){
        recordList.add(record);
    }
    public ArrayList<Record> getRecordList(){
        return (ArrayList<Record>) recordList.clone();
    }
    public void addSpout(String appType,String file,String functionType){
        Spout spout = new Spout(file,appType,functionType);
        spoutList.add(spout);
    }
    public void addBolt(String functiontype,String info,String appType){
        Bolt bolt = new Bolt(functiontype,info,appType);
        boltList.add(bolt);
    }
}
