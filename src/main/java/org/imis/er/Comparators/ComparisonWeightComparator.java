package org.imis.er.Comparators;

import org.imis.er.DataStructures.Comparison;
import java.util.Comparator;


public class ComparisonWeightComparator implements Comparator<Comparison> {

    @Override
    public int compare(Comparison o1, Comparison o2) {
        double test = o2.getUtilityMeasure()-o1.getUtilityMeasure(); 
        if (0 < test) {
            return -1;
        }

        if (test < 0) {
            return 1;
        }

        return 0;
    }
    
}