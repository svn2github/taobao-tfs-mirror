package com.taobao.common.tfs.nameMetaOper;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.io.FileReader;
import java.io.BufferedReader;

public class SectProp {                                                                                                                                                 
    private HashMap<String, HashMap<String, String>> sectMap = new HashMap<String, HashMap<String, String>>();

    public boolean load(String configFile) {
      BufferedReader buffReader = null;
      try {
        buffReader = new BufferedReader(new FileReader(configFile));
        String line = null;
        String equalSign = "=";
        String hashSign = "#";
        String sectName = "";
        while ((line = buffReader.readLine()) != null) {
          if (line.equals("") || line.startsWith(hashSign)) {
            continue;
          }
          line = line.trim();
          // section
          if (isSectName(line)) {
            sectName = line.substring(1, line.length()-1);
            HashMap<String, String> propMap = new HashMap<String, String>();
            sectMap.put(sectName, propMap); 
          }
          else {
            // property
            String[] props = line.split(equalSign);
            if (props.length != 2) {
              return false;
            }
            String propName = props[0].trim();
            String propValue = props[1].trim();
            if (sectName.equals("")) {
              return false;
            }
            HashMap<String, String> propMap = sectMap.get(sectName);
            if (null == propMap) {
              return false;
            }
            propMap.put(propName, propValue);
          }
        }
      } catch (Exception e) {
          e.printStackTrace();
      } finally {
        try {
          buffReader.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      return true;
    }

    public void list() {
      Iterator sectIter = sectMap.entrySet().iterator();
      while (sectIter.hasNext()) {
        Map.Entry<String, HashMap<String, String>> sectEntry = (Map.Entry<String, HashMap<String, String>>)sectIter.next();
        String sectName = sectEntry.getKey(); 
        HashMap<String, String> propMap = sectEntry.getValue();
        System.out.println("[" + sectName + "]");

        Iterator propIter = propMap.entrySet().iterator();
        while (propIter.hasNext()) {
          Map.Entry<String, String> propEntry = (Map.Entry<String, String>)propIter.next();
          String propName = propEntry.getKey(); 
          String propValue = propEntry.getValue(); 
          System.out.println(propName + " = " + propValue);
        }
      }
    }

    public String getPropValue(String sectName, String propName, String defaultValue) {
      if (null != sectName && null != propName) {
        HashMap<String, String> propMap = sectMap.get(sectName);
        String propValue = propMap.get(propName);
        if (propValue != null)
          return propValue;
      }
      return defaultValue;
    }

    private boolean isSectName(String line) {
      String leftBracket = "[";
      String rightBracket = "]";
      return (line.startsWith(leftBracket) && line.endsWith(rightBracket));
    }
}
