package team.cs425.g54;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class test1 {
    public static void main(String[] args) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("newtest.txt"));
            bufferedWriter.write("sdfsdfsdfsdfsf");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
