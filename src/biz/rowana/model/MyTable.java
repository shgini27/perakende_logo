/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package biz.rowana.model;

import javax.swing.table.DefaultTableModel;

/**
 *
 * @author Shagy
 */
public class MyTable extends DefaultTableModel{
    
    public MyTable(){
        
    }

    @Override
    public boolean isCellEditable(int row, int column) {
        return column == 3;
    }
}
