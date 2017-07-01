package br.com.mapreduce;

import java.io.IOException;

public class OISERAQEUSOUUTIL {
    public static void main(String[] args) {

        System.out.println("Executing CommandRemoteExecuter.............");
        try {
            Runtime runtime = Runtime.getRuntime();
            runtime.exec("touch /home/luisfelipeas5/hadoop-2.6.4/teste");
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("CommandRemoteExecuter Finished.............");

    }
}
