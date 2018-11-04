package team.cs425.g54;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import static team.cs425.g54.MsgHandler.logger;

public class StoreInfo {
    public CopyOnWriteArrayList<String> fileLists;
    public Hashtable<String,CopyOnWriteArrayList<String>> fileVersions;
    Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");
    final int max_versions = 20;
    Node mynode;
    public void initFileLists(Node node){
        mynode = node;
        fileLists = new CopyOnWriteArrayList<>();
        fileVersions = new Hashtable<>();
        File dict = new File(Detector.SDFSPath); // get all sdfs file
        File[] fileArray = dict.listFiles();
        if(fileArray==null)
            return;
        System.out.println("Current store file");
        for(int i=0;i<fileArray.length;i++){
            if(fileArray[i].isFile()){
                String[] sdfsFile = fileArray[i].getName().split("_");
                if(sdfsFile.length<2)
                    continue;
                String name = sdfsFile[0];
                String version = sdfsFile[1];
                if(!fileLists.contains(name)) {
                    fileLists.add(name);
                    CopyOnWriteArrayList<String> tmp = new CopyOnWriteArrayList<>();
                    tmp.add(version);
                    fileVersions.put(name,tmp);
                }
                if(fileLists.contains(name)) {
                    if (!fileVersions.get(name).contains(version))
                        fileVersions.get(name).add(version);
                }
            }
        }
    }
    public void addFileUpdate(String name, String timestamp){
        if(!fileLists.contains(name)) {
            fileLists.add(name);
            CopyOnWriteArrayList<String> tmp = new CopyOnWriteArrayList<>();
            tmp.add(timestamp);
            fileVersions.put(name,tmp);
        }
        if (!fileVersions.get(name).contains(timestamp))
            fileVersions.get(name).add(timestamp);
//        while(fileVersions.get(name).size()>max_versions) {
//            String deleteName = name+"_"+fileVersions.get(name).get(0);
//            File deleteFile = new File(deleteName);  // delete old version
//            if(deleteFile.delete()){
//                logger.info("delete old version "+deleteName+" successfully..");
//            }
//            else
//                logger.info("file delete failed...");
//            fileVersions.remove(0);
//        }

    }
    public void deleteFileUpdate(String name){
        if(!fileLists.contains(name)) {
            logger.info("no such file to delete");
            return ;
        }
        deleteFileAllVersionsOnDisk(name);
        fileLists.remove(name);
        fileVersions.remove(name);
    }
    public void showFiles(){
        logger.info("Node "+mynode.nodeID+" current store sdfs files:" );
        for(String file:fileLists){
            System.out.println(file);
        }
        System.out.println();
    }

    public boolean hasFile(String name){
        return fileLists.contains(name);
    }

    public ArrayList<String> getKVersions(String name,int k){
        if(!fileLists.contains(name))
            return null;
        CopyOnWriteArrayList<String> versions = fileVersions.get(name);
        ArrayList<String> result = new ArrayList<>();
        for(int i=versions.size()-1;i>=0;i--){
            if(k==0)
                break;
            result.add(versions.get(i));
            k--;
        }
        return result;
    }
    public String getLatestVersion(String name){
        logger.info("Getting sdfsfile "+name);
        if(fileVersions.containsKey(name))
            return fileVersions.get(name).get(fileVersions.get(name).size()-1);
        return "NULL";
    }
    public void deleteAllSDFSFilesOnDisk(){
        for(String file:fileLists){
            for(String version:fileVersions.get(file)){
                File deletefile = new File(Detector.SDFSPath+file+"_"+version);
                if(deletefile.delete())
                    logger.info("delete file "+file+"_"+version+"success");
            }
        }
        fileLists.clear();
        fileVersions.clear();
    }

    public void deleteFileAllVersionsOnDisk(String name){
        for(String version:fileVersions.get(name)){
            String deletefile = name+"_"+version;
            File file = new File(Detector.SDFSPath+deletefile);
            file.delete();
        }
    }
    public CopyOnWriteArrayList<String> getAllFiles(){
        return fileLists;
    }
    public Hashtable<String,CopyOnWriteArrayList<String>> getAllVersions(){
        return fileVersions;
    }
}
