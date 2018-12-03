package team.cs425.g54.topology;

public class Bolt {
    public String functionType;
    public String appType;
    public String info; // probable input info
    public Bolt(String functionType,String info,String appType){
        this.appType = appType;
        this.functionType = functionType;
        this.info = info;
    }
}
