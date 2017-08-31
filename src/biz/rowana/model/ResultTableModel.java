/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.rowana.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.swing.table.AbstractTableModel;

/**
 *
 * @author Shagy
 */
public class ResultTableModel extends AbstractTableModel{
    
    //decleration
    private ResultSet rs;
    
    public ResultTableModel(){
        
    }
    public ResultTableModel(ResultSet rs){
        this.rs = rs;
    }
    
    //metod to get column count
    @Override
    public int getColumnCount(){
        try{
            if(rs == null){
                return 0;
            }else{
                return rs.getMetaData().getColumnCount();
            }
        }catch(SQLException e){
            System.out.println("getColumnCount resultset generating error while "
                    + "getting column count");
            System.out.println(e.getMessage());
            return 0;
        }
    }
    
    //metod to get row count
    @Override
    public int getRowCount(){
        int rows;
        try{
            if(rs == null){
                return 0;
            }else{
                rs.last();
                rows = rs.getRow();
                return rows;
            }
        }catch(SQLException e){
            System.out.println("getRowCount resultset generating error while "
                    + "getting rows count");
            System.out.println(e.getMessage());
            return 0;
        }
    }
    
    @Override
    public Object getValueAt(int rowIndex, int columnIndex){
        if(rowIndex < 0 || rowIndex > getRowCount() || columnIndex < 0 
                || columnIndex > getColumnCount()){
            
            return null;
        }
        try{
            if(rs == null){
                return null;
            }else{
                rs.absolute(rowIndex + 1);
                return rs.getObject(columnIndex + 1);
            }
        }catch(SQLException e){
            System.out.println("getValueAt resultset generating error while fetching rows");
            System.out.println(e.getMessage());
            return null;
        }
    }
    
//    //Deneme
//    public Object getValueAt(int [] rowIndexes, int columnIndex){
//        Object [] data = null;
//        for(int i = 0; i < rowIndexes.length; i++){
//            try {
//                rs.absolute(rowIndexes[i]);
//                data[i] = (Object)rs.getObject(columnIndex);
//            } catch (SQLException ex) {
//                System.out.println("getValueAt resultset generating error while "
//                        + "fetching rows");
//                System.out.println(ex.getMessage());
//            }
//        }
//        return data;
//    }

    @Override
    public String getColumnName(int columnIndex) {
        try{
            return rs.getMetaData().getColumnName(columnIndex + 1);
        }catch(SQLException e){
            System.out.println("getColumnName resultset generating error while "
                    + "fetching column name");
            System.out.println(e.getMessage());
        }
        return super.getColumnName(columnIndex);
    }
}
