/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package org.imis.er.EfficiencyLayer.BlockRefinement;


import org.imis.er.Comparators.BlockUtilityComparator;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.EfficiencyLayer.AbstractEfficiencyMethod;

import java.util.Collections;
import java.util.List;



public class BlockScheduling extends AbstractEfficiencyMethod {
    
    public BlockScheduling() {
        super("Block Scheduling");
    }
    
    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        for (AbstractBlock block : blocks) {
            block.setUtilityMeasure();
        }
        Collections.sort(blocks, new BlockUtilityComparator());
    }
} 
