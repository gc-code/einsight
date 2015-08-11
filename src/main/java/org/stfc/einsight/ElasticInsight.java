package org.stfc.einsight;

public class ElasticInsight {
    private String hostname = "localhost";
    private String clustername = "sawmill";

    public String getArgument(String[] args, int index) {
        if (index + 1 > args.length - 1) {
            System.out.println("ERROR: no argument passed for option at the command line");
            System.exit(1);
        }
        return args[index+1];
    }

    public void parseOptions(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--hostname")) {
                hostname = getArgument(args, i);
                i++;
            } else if (args[i].equals("-c") || args[i].equals("--cluster")) {
                clustername = getArgument(args, i);
                i++;
            } else {
                System.out.println("ERROR: Invalid option passed at the command line");
                System.exit(1);
            }
        }
    }

    public String getHostname() { return hostname; }
    public String getClustername() { return clustername; }

    public static void main(String[] args) {
        ElasticInsight elIns = new ElasticInsight();
        elIns.parseOptions(args);

        ElasticInterface elInt = new ElasticInterface();
        elInt.connect(elIns.getHostname(), elIns.getClustername());
        elInt.runLoadQuery();
    }
}
