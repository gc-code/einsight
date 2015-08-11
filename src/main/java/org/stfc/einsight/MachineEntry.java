package org.stfc.einsight;

public class MachineEntry {
    private String machineName;
    private int storageTb;
    private int numRequestsReceived = 0;

    public void setMachineName(String name) { machineName = name; }
    public String getMachineName() { return machineName; }

    public void setStorageTb(int size) { storageTb = size; }
    public int getStorageTb() { return storageTb; }

    public void setNumRequestsReceived(int num) { numRequestsReceived = num; }
    public int getNumRequestsReceived() { return numRequestsReceived; }

    public void print() {
        System.out.println("Machine name: " + machineName);
        System.out.println("Storage size: " + storageTb);
        System.out.println("Number of requests: " + numRequestsReceived);
        System.out.println("------------------------");
    }
}
