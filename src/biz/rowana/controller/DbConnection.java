/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.rowana.controller;

import java.awt.HeadlessException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 *
 * @author Shagy
 */
public class DbConnection {

    private String url;
    private String userName;
    private String password;

    //default Constructor
    public DbConnection() {

    }

    //Custom Constructor
    public DbConnection(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    /**
     * Method for getting connection to database.
     *
     * @return Connection from database.
     */
    public Connection connectDatabase() {
        Connection connection = null;
        try {
            System.out.println("Conecting...");
            if (url == null || userName == null || password == null) {
                JOptionPane.showMessageDialog(null, "Lutfen database baglanty "
                        + "bilgileri giriniz!");
            } else {
                connection = DriverManager.getConnection(url, userName, password);
                System.out.println("Conected!");
            }

        } catch (HeadlessException | SQLException e) {
            JOptionPane.showMessageDialog(null, "DATABASA BAGLANMADY!");
            System.err.println(e.getMessage());
        }
        return connection;
    }

    /**
     * Method for closing connection.
     *
     * @param connection from database.
     */
    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        System.out.println("Connection closed!!!");
    }

    /**
     * Method for closing resultSet.
     *
     * @param resultSet from database.
     */
    public void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
        System.out.println("ResultSet is closed!!!");
    }

    /**
     * Method for getting ResultSet from connected database.
     *
     * @param connection from database.
     * @param sql query to be executed.
     * @return ResultSet
     */
    public ResultSet executionQuery(Connection connection, String sql) {
        try {
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);//check this out
            return statement.executeQuery(sql);
        } catch (SQLException ex) {
            System.err.println(ex);
            return null;
        }
    }

    /**
     * Method to view specific clause of table values.
     *
     * @param tableName table name that will be queried.
     * @param columnName query condition to be executed.
     * @param connection query to be executed.
     * @return ResultSet
     */
    public ResultSet querySelect(Connection connection, String[] columnName, String tableName) {
        String sql = "SELECT ";

        for (int i = 0; i <= columnName.length - 1; i++) {
            sql += columnName[i];
            if (i < columnName.length - 1) {
                sql += ",";
            }
        }

        sql += " FROM " + tableName;
        System.out.println("SQL: " + sql);
        return this.executionQuery(connection, sql);
    }

    //method to get product by its barcode
    public String[] getProductByBarcode(Connection connection, String tableNameCode, String barcode) {
        String[] product = {"-1", "-1"};
        ResultSet resultSet = null;

        String sql = "SELECT CODE, NAME FROM " + tableNameCode + "ITEMS WHERE "
                + "LOGICALREF = (SELECT ITEMREF FROM " + tableNameCode + ""
                + "UNITBARCODE WHERE BARCODE = '" + barcode + "')";
        System.out.println("query: " + sql);
        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                product[0] = resultSet.getString("CODE");
                product[1] = resultSet.getString("NAME");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return product;
    }

    //method to get price by barcode
    //method to get product by name or code
    public String[] getProduct(Connection connection, String tableNameCode, String productName) {

        String[] product = {"-1", "-1"};
        ResultSet resultSet = null;
        String sql = "SELECT CODE, NAME FROM " + tableNameCode + "ITEMS \n"
                + "            WHERE NAME = '" + productName + "' OR CODE = '" + productName + "'";
        System.out.println("query: " + sql);
        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                product[0] = resultSet.getString("CODE");
                product[1] = resultSet.getString("NAME");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return product;
    }

    public ResultSet getPriceByBarcode(Connection connection, String barcode) {
        String sql = "SELECT * FROM MS_FIYAT_LISTE WHERE BARKODY = '" + barcode + "'";

        System.out.println("query: " + sql);

        return this.executionQuery(connection, sql);
    }

    public Double getPrice(Connection connection, String tableNameCode, String productName) {

        Double price = 0.0;
        ResultSet resultSet = null;

        String sql = "SELECT PRICE FROM " + tableNameCode + "PRCLIST WHERE CARDREF = "
                + "(SELECT LOGICALREF FROM " + tableNameCode + "ITEMS WHERE CODE = "
                + "'" + productName + "' OR NAME = '" + productName + "' AND PTYPE = "
                + "'2') AND LOGICALREF = (SELECT MAX(LOGICALREF) "
                + "FROM " + tableNameCode + "PRCLIST WHERE CARDREF = (SELECT LOGICALREF "
                + "FROM " + tableNameCode + "ITEMS WHERE CODE = '" + productName + "' "
                + "OR NAME = '" + productName + "' AND PTYPE = '2'))";
        System.out.println("query: " + sql);

        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                price = resultSet.getDouble("PRICE");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return price;
    }

    //METHOD TO GET PRICE FROM VIEW
    public ResultSet getPrice(Connection connection, String productName) {

        String sql = "SELECT * FROM MS_FIYAT_LISTE WHERE HARYT_KODY LIKE "
                + "'%" + productName + "%' OR HARYT_ADY LIKE '%" + productName + "%'";

        System.out.println("query: " + sql);

        return this.executionQuery(connection, sql);
    }

    public String getUnit(Connection connection, String tableNameCode, String productName) {

        String price = "";
        ResultSet resultSet = null;

        String sql = "SELECT CODE FROM " + tableNameCode + "UNITSETL WHERE "
                + "LOGICALREF = (SELECT UOMREF FROM " + tableNameCode + "PRCLIST WHERE CARDREF = "
                + "(SELECT LOGICALREF FROM " + tableNameCode + "ITEMS WHERE CODE = "
                + "'" + productName + "' OR NAME = '" + productName + "' AND PTYPE = "
                + "'2') AND LOGICALREF = (SELECT MAX(LOGICALREF) "
                + "FROM " + tableNameCode + "PRCLIST WHERE CARDREF = (SELECT LOGICALREF "
                + "FROM " + tableNameCode + "ITEMS WHERE CODE = '" + productName + "' "
                + "OR NAME = '" + productName + "' AND PTYPE = '2')))";

        System.out.println("query: " + sql);
        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                price = resultSet.getString("CODE");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return price;
    }

    /**
     * Method to view specific clause of table values.
     *
     * @param tableName table name that will be queried.
     * @param columnName query condition to be executed.
     * @param connection query to be executed.
     * @param state query to be executed.
     * @return ResultSet
     */
    public ResultSet selectCommand(Connection connection, String[] columnName,
            String tableName, String state) {
        String sql = "SELECT ";

        for (int i = 0; i <= columnName.length - 1; i++) {
            sql += columnName[i];
            if (i < columnName.length - 1) {
                sql += ",";
            }
        }

        sql += " FROM " + tableName + " WHERE " + state;
        System.out.println("SQL: " + sql);
        return this.executionQuery(connection, sql);
    }

    //metod for execution of application update
    public String executionUpdate(Connection connection, String sql, String tableName) {
        try {
            connection.createStatement().executeUpdate(sql);
            return sql;
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
            return ex.getMessage();
        }
    }

    //metod to view all
    public ResultSet querySelectAll(Connection connection, String tableName) {
        String sql = "SELECT * FROM " + tableName;
        System.out.println(sql);
        return executionQuery(connection, sql);
    }

    //get column names of given table
    public String[] getColumnNames(ResultSet resultSet) {
        String[] columnNames;
        ResultSetMetaData metaData;
        int count;

        try {
            metaData = resultSet.getMetaData();
            count = metaData.getColumnCount();
            columnNames = new String[count];

            for (int i = 1; i <= count; i++) {
                columnNames[i - 1] = metaData.getColumnName(i);
                //System.out.println(i + " Column Names: " + columnNames[i - 1]);
            }
            return columnNames;
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    // metod to get primaryKey for update metod
    public int getLogicalRef(Connection connection, String table,
            String searchCondition, String columKey, String columSearch) {

        int primaryKey = 0;
        ResultSet resultSet = null;
        String sql = "SELECT " + columKey + " FROM " + table + " WHERE "
                + columSearch + " = '" + searchCondition + "'";
        System.out.println("query: " + sql);
        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                primaryKey = resultSet.getInt(columKey);
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return primaryKey;
    }

    public int getPriceLogRef(Connection connection, String tableNameCode, String productName) {

        int logRef = 0;
        ResultSet resultSet = null;

        String sql = "SELECT LOGICALREF FROM " + tableNameCode + "PRCLIST WHERE CARDREF = "
                + "(SELECT LOGICALREF FROM " + tableNameCode + "ITEMS WHERE CODE = "
                + "'" + productName + "' OR NAME = '" + productName + "' AND PTYPE = "
                + "'2') AND LOGICALREF = (SELECT MAX(LOGICALREF) "
                + "FROM " + tableNameCode + "PRCLIST WHERE CARDREF = (SELECT LOGICALREF "
                + "FROM " + tableNameCode + "ITEMS WHERE CODE = '" + productName + "' "
                + "OR NAME = '" + productName + "' AND PTYPE = '2'))";
        System.out.println("query: " + sql);

        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                logRef = resultSet.getInt("LOGICALREF");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return logRef;
    }

    // metod to get primaryKey for update metod
    public int getLogicalRef(Connection connection, String tabelName, String ficheNo, int trCode) {

        int primaryKey = 0;
        ResultSet resultSet = null;
        String sql = "SELECT LOGICALREF FROM " + tabelName + " WHERE "
                + "LOGICALREF = (SELECT max(LOGICALREF) FROM " + tabelName + " WHERE "
                + "TRCODE = '" + trCode + "' AND FICHENO = '" + ficheNo + "')";
        System.out.println("query: " + sql);
        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                primaryKey = resultSet.getInt("LOGICALREF");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return primaryKey;
    }

    //GET FICHENO
    public String getFicheNo(Connection connection, String tableCode, String ficheCode, int trcode) {
        ResultSet resultSet = null;
        String ficheNo = null;

        String sql = "SELECT FICHENO FROM " + tableCode + "INVOICE WHERE "
                + "LOGICALREF = (SELECT max(LOGICALREF) FROM " + tableCode + "INVOICE "
                + "WHERE TRCODE = '" + trcode + "' AND FICHENO LIKE '" + ficheCode + "%')";

        try {
            resultSet = executionQuery(connection, sql);
            while (resultSet.next()) {
                ficheNo = resultSet.getString("FICHENO");
            }
        } catch (SQLException ex) {
            Logger.getLogger(DbConnection.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(0);
        } finally {
            closeResultSet(resultSet);
        }
        return ficheNo;
    }

    public int getInt(String str) {
        int fiche = 0;

        if (str != null) {
            String str2 = str.substring(3);
            for (int i = 0; i < str2.length(); i++) {
                if (Integer.valueOf(str2.substring(str2.length() - (i + 1))) > 0) {
                    fiche = Integer.valueOf(str2.substring(str2.length() - (i + 1)));
                }
            }
        }
        return fiche;
    }

    /**
     * **********************************INSERT DATA**************************
     */
    /**
     * Method to insert data to STFICHE table in the database.
     *
     * @param connection connection to the database.
     * @param tableName table name from the database.
     * @param ficheNo given fiche number.
     * @param fTime sale time.
     * @param trcode action code.
     * @param clientRef reference to client.
     * @param total sales amount.
     * @param netTotal total amount.
     * @param invoiceRef invoice reference number.
     * @return sql string
     *
     */
    public String insertFiche(Connection connection, String tableName, String ficheNo,
            String fTime, Double total, Double netTotal, String invoiceRef, int clientRef, int trcode) {
        String sql;

        ResultSet rs = querySelectAll(connection, tableName);
        String[] column = getColumnNames(rs);

        sql = "INSERT INTO " + tableName + "(";
        for (int i = 0; i <= column.length - 1; i++) {
            if (!column[i].equals("CAPIBLOCK_MODIFIEDDATE") && !column[i].equals("APPROVEDATE")
                    && !column[i].equals("SHIPDATE") && !column[i].equals("LOGICALREF")) {
                sql += column[i];
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("APPROVEDATE") && !column[i].equals("SHIPDATE")
                    && !column[i].equals("LOGICALREF")) {
                sql += ",";
            }
        }
        sql += ") VALUES (";

        for (int i = 0; i <= column.length - 1; i++) {
            switch (column[i]) {
                case "LOGICALREF":
                    break;
                case "CLIENTREF":
                    sql += "'" + clientRef + "'";
                    break;
                case "FICHENO":
                    sql += "'" + ficheNo + "'";
                    break;
                case "DATE_":
                    sql += "'" + fTime.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "FTIME":
                    int time = (Integer.valueOf(fTime.substring(11, 13)) * 65536 * 256
                            + (Integer.valueOf(fTime.substring(14, 16)) * 65536)
                            + (Integer.valueOf(fTime.substring(17, 19)) * 256));
                    sql += "'" + time + "'";
                    break;
                case "INVNO":
                    sql += "'" + ficheNo + "'";
                    break;
                case "ADDDISCOUNTS":
                    sql += "'" + String.format("%.2f", (total - netTotal)) + "'";
                    break;
                case "TOTALDISCOUNTS":
                    sql += "'" + String.format("%.2f", (total - netTotal)) + "'";
                    break;
                case "TOTALDISCOUNTED":
                    sql += "'" + total + "'";
                    break;
                case "GROSSTOTAL":
                    sql += "'" + total + "'";
                    break;
                case "NETTOTAL":
                    sql += "'" + netTotal + "'";
                    break;
                case "REPORTNET":
                    sql += "'" + netTotal + "'";
                    break;
                case "INVOICEREF":
                    sql += "'" + invoiceRef + "'";
                    break;
                case "GRPCODE":
                    sql += "'2'";
                    break;
                case "TRCODE":
                    sql += "'" + trcode + "'";
                    break;
                case "IOCODE":
                    if (trcode == 7) {
                        sql += "'3'";
                    } else {
                        sql += "'1'";
                    }
                    break;
                case "BILLED":
                    sql += "'1'";
                    break;
                case "REPORTRATE":
                    sql += "'1'";
                    break;
                case "FICHECNT":
                    sql += "'1'";
                    break;
                case "CAPIBLOCK_CREATEDBY":
                    sql += "'1'";
                    break;
                case "CAPIBLOCK_CREADEDDATE":
                    sql += "'" + fTime + "'";
                    break;
                case "CAPIBLOCK_CREATEDHOUR":
                    sql += "'" + fTime.substring(11, 13) + "'";
                    break;
                case "CAPIBLOCK_CREATEDMIN":
                    sql += "'" + fTime.substring(14, 16) + "'";
                    break;
                case "CAPIBLOCK_CREATEDSEC":
                    sql += "'" + fTime.substring(17, 19) + "'";
                    break;
                case "CAPIBLOCK_MODIFIEDDATE":
                    break;
                case "APPROVEDATE":
                    break;
                case "SHIPDATE":
                    break;
                case "GENEXCTYP":
                    sql += "'2'";
                    break;
                case "DEDUCTIONPART1":
                    sql += "'2'";
                    break;
                case "DEDUCTIONPART2":
                    sql += "'3'";
                    break;
                case "AFFECTRISK":
                    sql += "'1'";
                    break;
                case "DISPSTATUS":
                    sql += "'1'";
                    break;
                case "DOCODE":
                    sql += "' '";
                    break;
                case "GLOBALID":
                    sql += "' '";
                    break;
                case "CAMPAIGNCODE":
                    sql += "' '";
                    break;
                case "SPECODE":
                    sql += "' '";
                    break;
                case "CYPHCODE":
                    sql += "' '";
                    break;
                case "GENEXP1":
                    sql += "' '";
                    break;
                case "GENEXP2":
                    sql += "' '";
                    break;
                case "GENEXP3":
                    sql += "' '";
                    break;
                case "GENEXP4":
                    sql += "' '";
                    break;
                case "PORDERFICHENO":
                    sql += "' '";
                    break;
                case "FRGTYPCOD":
                    sql += "' '";
                    break;
                case "ORGLOGOID":
                    sql += "' '";
                    break;
                case "UGIRTRACKINGNO":
                    sql += "' '";
                    break;
                case "DOCTRACKINGNR":
                    sql += "' '";
                    break;
                case "TRADINGGRP":
                    sql += "' '";
                    break;
                case "TRACKNR":
                    sql += "' '";
                    break;
                case "SHPAGNCOD":
                    sql += "' '";
                    break;
                case "SHPTYPCOD":
                    sql += "' '";
                    break;
                default:
                    sql += "'0'";
                    break;
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("APPROVEDATE") && !column[i].equals("SHIPDATE")
                    && !column[i].equals("LOGICALREF")) {
                sql += ", ";
            }
        }
        sql += ")";

        System.out.println(sql);
        //this.executionUpdate(connection, sql, tableName);
        return this.executionUpdate(connection, sql, tableName);
    }

    public String insertStLine(Connection connection, String tableName, int clientRef,
            int ficheRef, int itemsRef, int uomRef, int usRef, int invoiceRef, Double amount,
            int prcLogRef, Double price, Double discountPercent, Double discount, Double total, int lineNo, int trcode,
            String time) {

        ResultSet rs = querySelectAll(connection, tableName);
        String[] column = getColumnNames(rs);

        Double subTotal = (amount * price);

        String sql;

        sql = "INSERT INTO " + tableName + "(";
        for (int i = 0; i <= column.length - 1; i++) {
            if (!column[i].equals("FAREGBINDDATE") && !column[i].equals("LOGICALREF")) {
                sql += column[i];
            }
            if (i < column.length - 1 && !column[i].equals("FAREGBINDDATE")
                    && !column[i].equals("LOGICALREF")) {
                sql += ",";
            }
        }
        sql += ") VALUES (";
        for (int i = 0; i <= column.length - 1; i++) {
            switch (column[i]) {
                case "LOGICALREF":
                    break;
                case "CLIENTREF":
                    sql += "'" + clientRef + "'";
                    break;
                case "STOCKREF":
                    sql += "'" + itemsRef + "'";
                    break;
                case "PRCLISTREF":
                    sql += "'" + prcLogRef + "'";
                    break;
                case "STFICHEREF":
                    sql += "'" + ficheRef + "'";
                    break;
                case "INVOICEREF":
                    sql += "'" + invoiceRef + "'";
                    break;
                case "UOMREF":
                    sql += "'" + uomRef + "'";
                    break;
                case "USREF":
                    sql += "'" + usRef + "'";
                    break;
                case "DATE_":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "FTIME":
                    int fTime = (Integer.valueOf(time.substring(11, 13)) * 65536 * 256
                            + (Integer.valueOf(time.substring(14, 16)) * 65536)
                            + (Integer.valueOf(time.substring(17, 19)) * 256));
                    if (ficheRef != 0) {
                        sql += "'" + fTime + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "FAREGBINDDATE":
                    break;
                case "MONTH_":
                    sql += "'" + time.substring(5, 7) + "'";
                    break;
                case "YEAR_":
                    sql += "'" + time.substring(0, 4) + "'";
                    break;
                case "STFICHELNNO":
                    if (ficheRef != 0) {
                        sql += "'" + lineNo + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "INVOICELNNO":
                    if (invoiceRef != 0) {
                        sql += "'" + lineNo + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "AMOUNT":
                    sql += "'" + amount + "'";
                    break;
                case "PRICE":
                    sql += "'" + price + "'";
                    break;
                case "PRPRICE":
                    sql += "'" + price + "'";
                    break;
                case "TOTAL":
                    if (itemsRef != 0) {
                        sql += "'" + String.format("%.2f", subTotal) + "'";
                    } else {
                        sql += "'" + discount + "'";
                    }
                    break;
                case "DISTCOST":
                    if ((subTotal != 0 && discount != 0.00) || total != 0.00) {
                        sql += "'" + String.format("%.2f", ((subTotal * discount) / total)) + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "DISTDISC":
                    if ((subTotal != 0 && discount != 0.00) || total != 0.00) {
                        sql += "'" + String.format("%.2f", ((subTotal * discount) / total)) + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "DISCPER":
                    if (itemsRef != 0) {
                        sql += "'0'";
                    } else {
                        sql += "'" + discountPercent + "'";
                    }
                    break;
                case "VATMATRAH":
                    if (itemsRef != 0) {
                        sql += "'" + String.format("%.2f", (subTotal - (subTotal * discount) / total)) + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "LINENET":
                    if (itemsRef != 0) {
                        sql += "'" + String.format("%.2f", (subTotal - (subTotal * discount) / total)) + "'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "LINETYPE":
                    if (itemsRef != 0) {
                        sql += "'0'";
                    } else {
                        sql += "'2'";
                    }
                    break;
                case "TRCODE":
                    sql += "'" + trcode + "'";
                    break;
                case "REPORTRATE":
                    sql += "'1'";
                    break;
                case "RECSTATUS":
                    sql += "'1'";
                    break;
                case "GLOBTRANS":
                    if (itemsRef != 0) {
                        sql += "'0'";
                    } else {
                        sql += "'1'";
                    }
                    break;
                case "IOCODE":
                    if (ficheRef != 0) {
                        if (trcode == 7) {
                            sql += "'4'";
                        } else {
                            sql += "'1'";
                        }
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "UINFO1":
                    if (itemsRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "UINFO2":
                    if (itemsRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "VATINC":
                    if (itemsRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "BILLED":
                    if (ficheRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "RETCOSTTYPE":
                    if (trcode == 2 && itemsRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "AFFECTRISK":
                    if (ficheRef != 0) {
                        sql += "'1'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "PRCURR":
                    if (itemsRef != 0) {
                        sql += "'158'";
                    } else {
                        sql += "'0'";
                    }
                    break;
                case "SPECODE":
                    sql += "' '";
                    break;
                case "ORGLOGOID":
                    sql += "' '";
                    break;
                case "GLOBALID":
                    sql += "' '";
                    break;
                case "SPECODE2":
                    sql += "' '";
                    break;
                case "VATEXCEPTREASON":
                    sql += "' '";
                    break;
                case "PLNDEFSERILOTNO":
                    sql += "' '";
                    break;
                case "DEDUCTCODE":
                    sql += "' '";
                    break;
                case "EXIMFICHENO":
                    sql += "' '";
                    break;
                case "OUTPUTIDCODE":
                    sql += "' '";
                    break;
                case "LINEEXP":
                    sql += "' '";
                    break;
                case "DELVRYCODE":
                    sql += "' '";
                    break;
                default:
                    sql += "'0'";
                    break;
            }
            if (i < column.length - 1 && !column[i].equals("FAREGBINDDATE")
                    && !column[i].equals("LOGICALREF")) {
                sql += ", ";
            }
        }
        sql += ")";
        System.out.println(sql);
        //this.executionUpdate(connection, sql, tableName)
        return this.executionUpdate(connection, sql, tableName);
    }

    public String insertInvoice(Connection connection, String tableName, int clientRef,
            String ficheNo, String time, Double discount, String total,
            String netTotal, int trcode, String borrow) {
        String sql;

        ResultSet rs = querySelectAll(connection, tableName);
        String[] column = getColumnNames(rs);

        sql = "INSERT INTO " + tableName + "(";
        for (int i = 0; i <= column.length - 1; i++) {
            if (!column[i].equals("CAPIBLOCK_MODIFIEDDATE") && !column[i].equals("APPROVEDATE")
                    && !column[i].equals("ESTARTDATE") && !column[i].equals("EENDDATE")
                    && !column[i].equals("LOGICALREF")) {
                sql += column[i];
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("APPROVEDATE") && !column[i].equals("ESTARTDATE")
                    && !column[i].equals("EENDDATE") && !column[i].equals("LOGICALREF")) {
                sql += ",";
            }
        }
        sql += ") VALUES (";

        for (int i = 0; i <= column.length - 1; i++) {
            switch (column[i]) {
                case "LOGICALREF":
                    break;
                case "CLIENTREF":
                    sql += "'" + clientRef + "'";
                    break;
                case "FICHENO":
                    sql += "'" + ficheNo + "'";
                    break;
                case "DATE_":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "TIME_":
                    int time1 = (Integer.valueOf(time.substring(11, 13)) * 65536 * 256
                            + (Integer.valueOf(time.substring(14, 16)) * 65536)
                            + (Integer.valueOf(time.substring(17, 19)) * 256));
                    sql += "'" + time1 + "'";
                    break;
                case "ADDDISCOUNTS":
                    sql += "'" + discount + "'";
                    break;
                case "TOTALDISCOUNTS":
                    sql += "'" + discount + "'";
                    break;
                case "TOTALDISCOUNTED":
                    sql += "'" + total + "'";
                    break;
                case "GROSSTOTAL":
                    sql += "'" + total + "'";
                    break;
                case "NETTOTAL":
                    sql += "'" + netTotal + "'";
                    break;
                case "TRNET":
                    sql += "'" + netTotal + "'";
                    break;
                case "REPORTNET":
                    sql += "'" + netTotal + "'";
                    break;
                case "GVATINC":
                    sql += "'1'";
                    break;
                case "ENTEGSET":
                    sql += "'247'";
                    break;
                case "GRPCODE":
                    sql += "'2'";
                    break;
                case "TRCODE":
                    sql += "'" + trcode + "'";
                    break;
                case "REPORTRATE":
                    sql += "'1'";
                    break;
                case "RECSTATUS":
                    sql += "'1'";
                    break;
                case "CAPIBLOCK_CREATEDBY":
                    sql += "'1'";
                    break;
                case "CAPIBLOCK_CREADEDDATE":
                    sql += "'" + time + "'";
                    break;
                case "DOCDATE":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "CAPIBLOCK_CREATEDHOUR":
                    sql += "'" + time.substring(11, 13) + "'";
                    break;
                case "CAPIBLOCK_CREATEDMIN":
                    sql += "'" + time.substring(14, 16) + "'";
                    break;
                case "CAPIBLOCK_CREATEDSEC":
                    sql += "'" + time.substring(17, 19) + "'";
                    break;
                case "CAPIBLOCK_MODIFIEDDATE":
                    break;
                case "APPROVEDATE":
                    break;
                case "ESTARTDATE":
                    break;
                case "EENDDATE":
                    break;
                case "GENEXCTYP":
                    sql += "'2'";
                    break;
                case "DEDUCTIONPART1":
                    sql += "'2'";
                    break;
                case "DEDUCTIONPART2":
                    sql += "'3'";
                    break;
                case "AFFECTRISK":
                    sql += "'1'";
                    break;
                case "DOCODE":
                    sql += "' '";
                    break;
                case "GLOBALID":
                    sql += "' '";
                    break;
                case "FRGTYPCOD":
                    sql += "' '";
                    break;
                case "PASSPORTNO":
                    sql += "' '";
                    break;
                case "CREDITCARDNO":
                    sql += "' '";
                    break;
                case "CREDITCARDNUM":
                    sql += "' '";
                    break;
                case "SPECODE":
                    sql += "' '";
                    break;
                case "CYPHCODE":
                    sql += "' '";
                    break;
                case "GENEXP1":
                    sql += "'" + borrow + "'";
                    break;
                case "GENEXP2":
                    sql += "' '";
                    break;
                case "GENEXP3":
                    sql += "' '";
                    break;
                case "GENEXP4":
                    sql += "' '";
                    break;
                case "ORGLOGOID":
                    sql += "' '";
                    break;
                case "DOCTRACKINGNR":
                    sql += "' '";
                    break;
                case "TRADINGGRP":
                    sql += "' '";
                    break;
                case "TRACKNR":
                    sql += "' '";
                    break;
                case "SHPAGNCOD":
                    sql += "' '";
                    break;
                case "SHPTYPCOD":
                    sql += "' '";
                    break;
                case "EDESCRIPTION":
                    sql += "' '";
                    break;
                case "CANCELEXP":
                    sql += "' '";
                    break;
                case "UNDOEXP":
                    sql += "' '";
                    break;
                case "VATEXCEPTREASON":
                    sql += "' '";
                    break;
                case "CAMPAIGNCODE":
                    sql += "' '";
                    break;
                case "SERIALCODE":
                    sql += "' '";
                    break;
                default:
                    sql += "'0'";
                    break;
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("APPROVEDATE") && !column[i].equals("ESTARTDATE")
                    && !column[i].equals("EENDDATE") && !column[i].equals("LOGICALREF")) {
                sql += ", ";
            }
        }
        sql += ")";

        System.out.println(sql);
        //this.executionUpdate(connection, sql, tableName);
        return this.executionUpdate(connection, sql, tableName);
    }

    public String insertClfLine(Connection connection, String tableName, int ficheRef,
            int clientRef, String ficheNo, Double amount, int trcode, String time) {

        ResultSet rs = querySelectAll(connection, tableName);
        String[] column = getColumnNames(rs);

        String sql;

        sql = "INSERT INTO " + tableName + "(";
        for (int i = 0; i <= column.length - 1; i++) {
            if (!column[i].equals("CAPIBLOCK_MODIFIEDDATE") && !column[i].equals("LOGICALREF")
                    && !column[i].equals("DEVIRPROCDATE") && !column[i].equals("DOCDATE")) {
                sql += column[i];
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("LOGICALREF") && !column[i].equals("DEVIRPROCDATE")
                    && !column[i].equals("DOCDATE")) {
                sql += ",";
            }
        }
        sql += ") VALUES (";
        for (int i = 0; i <= column.length - 1; i++) {
            switch (column[i]) {
                case "LOGICALREF":
                    break;
                case "CLIENTREF":
                    sql += "'" + clientRef + "'";
                    break;
                case "SOURCEFREF":
                    sql += "'" + ficheRef + "'";
                    break;
                case "TRANNO":
                    sql += "'" + ficheNo + "'";
                    break;
                case "DATE_":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "FTIME":
                    int time1 = (Integer.valueOf(time.substring(11, 13)) * 65536 * 256
                            + (Integer.valueOf(time.substring(14, 16)) * 65536)
                            + (Integer.valueOf(time.substring(17, 19)) * 256));
                    sql += "'" + time1 + "'";
                    break;
                case "MONTH_":
                    sql += "'" + time.substring(5, 7) + "'";
                    break;
                case "YEAR_":
                    sql += "'" + time.substring(0, 4) + "'";
                    break;
                case "CAPIBLOCK_CREATEDBY":
                    sql += "'1'";
                    break;
                case "CAPIBLOCK_CREADEDDATE":
                    sql += "'" + time + "'";
                    break;
                case "CAPIBLOCK_CREATEDHOUR":
                    sql += "'" + time.substring(11, 13) + "'";
                    break;
                case "CAPIBLOCK_CREATEDMIN":
                    sql += "'" + time.substring(14, 16) + "'";
                    break;
                case "CAPIBLOCK_CREATEDSEC":
                    sql += "'" + time.substring(17, 19) + "'";
                    break;
                case "CAPIBLOCK_MODIFIEDDATE":
                    break;
                case "AMOUNT":
                    sql += "'" + amount + "'";
                    break;
                case "REPORTNET":
                    sql += "'" + amount + "'";
                    break;
                case "TRNET":
                    sql += "'" + amount + "'";
                    break;
                case "TRCODE":
                    if (trcode == 7) {
                        sql += "'37'";
                    } else {
                        sql += "'32'";
                    }
                    break;
                case "SIGN":
                    if (trcode == 7) {
                        sql += "'0'";
                    } else {
                        sql += "'1'";
                    }
                    break;
                case "MODULENR":
                    sql += "'4'";
                    break;
                case "REPORTRATE":
                    sql += "'1'";
                    break;
                case "AFFECTRISK":
                    sql += "'1'";
                    break;
                case "SPECODE":
                    sql += "' '";
                    break;
                case "CYPHCODE":
                    sql += "' '";
                    break;
                case "DOCODE":
                    sql += "' '";
                    break;
                case "LINEEXP":
                    sql += "' '";
                    break;
                case "TRADINGGRP":
                    sql += "' '";
                    break;
                case "CHEQINFO":
                    sql += "' '";
                    break;
                case "CREDITCNO":
                    sql += "' '";
                    break;
                case "BATCHNUM":
                    sql += "' '";
                    break;
                case "APPROVENUM":
                    sql += "' '";
                    break;
                case "ORGLOGOID":
                    sql += "' '";
                    break;
                case "DEVIRPROCDATE":
                    break;
                case "DOCDATE":
                    break;
                default:
                    sql += "'0'";
                    break;
            }
            if (i < column.length - 1 && !column[i].equals("CAPIBLOCK_MODIFIEDDATE")
                    && !column[i].equals("LOGICALREF") && !column[i].equals("DEVIRPROCDATE")
                    && !column[i].equals("DOCDATE")) {
                sql += ", ";
            }
        }
        sql += ")";
        System.out.println(sql);
        //this.executionUpdate(connection, sql, tableName)
        return this.executionUpdate(connection, sql, tableName);
    }

    public String insertPaytrans(Connection connection, String tableName, int ficheRef,
            int clientRef, int trcode, Double amount, String time) {

        ResultSet rs = querySelectAll(connection, tableName);
        String[] column = getColumnNames(rs);

        String sql;

        sql = "INSERT INTO " + tableName + "(";
        for (int i = 0; i <= column.length - 1; i++) {
            if (!column[i].equals("BANKPAYDATE") && !column[i].equals("LOGICALREF")
                    && !column[i].equals("VALBEGDATE") && !column[i].equals("DEVIRPROCDATE")
                    && !column[i].equals("MATCHDATE")) {
                sql += column[i];
            }
            if (i < column.length - 1 && !column[i].equals("BANKPAYDATE")
                    && !column[i].equals("LOGICALREF") && !column[i].equals("VALBEGDATE")
                    && !column[i].equals("DEVIRPROCDATE") && !column[i].equals("MATCHDATE")) {
                sql += ",";
            }
        }
        sql += ") VALUES (";
        for (int i = 0; i <= column.length - 1; i++) {
            switch (column[i]) {
                case "LOGICALREF":
                    break;
                case "BANKPAYDATE":
                    break;
                case "CARDREF":
                    sql += "'" + clientRef + "'";
                    break;
                case "FICHEREF":
                    sql += "'" + ficheRef + "'";
                    break;
                case "DATE_":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "PROCDATE":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "DISCDUEDATE":
                    sql += "'" + time.substring(0, 10) + " 00:00:00.000'";
                    break;
                case "TOTAL":
                    sql += "'" + amount + "'";
                    break;
                case "TRCODE":
                    sql += "'" + trcode + "'";
                    break;
                case "MODULENR":
                    sql += "'4'";
                    break;
                case "REPORTRATE":
                    sql += "'1'";
                    break;
                case "SIGN":
                    if(trcode == 2){
                        sql += "'1'";
                    }else{
                        sql += "'0'";
                    }
                    break;
                case "PAYNO":
                    sql += "'1'";
                    break;
                case "ORGLOGOID":
                    sql += "' '";
                    break;
                case "SPECODE":
                    sql += "' '";
                    break;
                case "CREDITCARDNUM":
                    sql += "' '";
                    break;
                case "RETREFNO":
                    sql += "' '";
                    break;
                case "BATCHNUM":
                    sql += "' '";
                    break;
                case "APPROVENUM":
                    sql += "' '";
                    break;
                case "POSTERMINALNUM":
                    sql += "' '";
                    break;
                case "DOCODE":
                    sql += "' '";
                    break;
                case "LINEEXP":
                    sql += "' '";
                    break;
                case "GLOBALCODE":
                    sql += "' '";
                    break;
                case "CLBNACCOUNTNO":
                    sql += "' '";
                    break;
                case "VALBEGDATE":
                    break;
                case "DEVIRPROCDATE":
                    break;
                case "MATCHDATE":
                    break;
                default:
                    sql += "'0'";
                    break;
            }
            if (i < column.length - 1 && !column[i].equals("BANKPAYDATE")
                    && !column[i].equals("LOGICALREF") && !column[i].equals("VALBEGDATE")
                    && !column[i].equals("DEVIRPROCDATE") && !column[i].equals("MATCHDATE")) {
                sql += ", ";
            }
        }
        sql += ")";
        System.out.println(sql);
        //this.executionUpdate(connection, sql, tableName)
        return this.executionUpdate(connection, sql, tableName);
    }
}
