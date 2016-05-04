/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package webminingfilter;

import java.util.ArrayList;

/**
 *
 * @author Kušický
 */
public interface FilterListener {
    
    void onFirstLoad(int allLines);
    void onUpdate(int actualLine, String status);
    void onFinish(int lines, ArrayList<Object> colums);
}
