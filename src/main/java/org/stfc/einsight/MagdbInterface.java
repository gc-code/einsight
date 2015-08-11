package org.stfc.einsight;

import java.sql.*;
import java.util.*;

public class MagdbInterface {
    private Connection conn = null;
    private ArrayList machineList = null;

    MagdbInterface() {
    }

    MagdbInterface(String hostname, String dbName) {
        connect();
    }

    public ArrayList getMachineList() {
        return machineList;
    }

    public void connect() {
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(
                "hostname",
                "username",
                "password");

            System.out.println("Connected to database");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void runLoadQuery() {
        String query = "SELECT * FROM \"vCastor5\"";
        Statement stmt = null;
        machineList = null;
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            machineList = new ArrayList();
            int i = 0;
            while (rs.next()) {
                MachineEntry newMachine = new MachineEntry();
                newMachine.setMachineName(rs.getString("machineName"));
                newMachine.setStorageTb(rs.getInt("sizeTb"));
                machineList.add(newMachine);
                i += 1;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

}
