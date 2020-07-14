package org.imis.er.MetaBlocking;


import java.util.List;

import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.Utilities.MetaBlockingConfiguration.WeightingScheme;

/**
 *
 * @author gap2
 */
public class EfficientEdgePruning extends EdgePruning {

    public EfficientEdgePruning() {
        super("Efficient Edge Pruning", WeightingScheme.CBS);
        averageWeight = 2.0;
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        getStatistics(blocks);
        filterComparisons(blocks);
    }
}
