package team.cs425.g54.topology;

import java.util.ArrayList;

public class Topology {
    ArrayList<Record> recordList;
    public void addRecode(Record record){
        recordList.add(record);
    }
    public ArrayList<Record> getRecordList(){
        return (ArrayList<Record>) recordList.clone();
    }
}
