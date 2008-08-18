/* 
 * ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2008
 * University of Konstanz, Germany
 * Chair for Bioinformatics and Information Mining (Prof. M. Berthold)
 * and KNIME GmbH, Konstanz, Germany
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * --------------------------------------------------------------------- *
 * 
 * History
 *   20.02.2008 (gabriel): created
 */
package org.knime.base.node.mine.bfn.fuzzy;

import org.knime.base.node.mine.bfn.BasisFunctionModelContent;
import org.knime.base.node.mine.bfn.BasisFunctionPortObject;
import org.knime.base.node.mine.bfn.BasisFunctionPredictorRow;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.PortType;

/**
 * 
 * @author Thomas Gabriel, University of Konstanz
 */
public final class FuzzyBasisFunctionPortObject 
        extends BasisFunctionPortObject {

    /** The <code>PortType</code> for basisfunction models. */
    public static final PortType TYPE = new PortType(
            FuzzyBasisFunctionPortObject.class);
    
    /**
     * 
     */
    public FuzzyBasisFunctionPortObject() {
        
    }
    
    /**
     * Creates a new basis function model object.
     * @param cont basisfunction model content containing rules and spec
     */
    public FuzzyBasisFunctionPortObject(final BasisFunctionModelContent cont) {
        super(cont);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BasisFunctionPortObject createPortObject(
            final BasisFunctionModelContent content) {
        return new FuzzyBasisFunctionPortObject(content);
    }
    
    /**
     * Used to create PNN predictor rows.
     */
    public static class FuzzyCreator implements Creator {
        /**
         * {@inheritDoc}
         */
        public BasisFunctionPredictorRow createPredictorRow(
                final ModelContentRO pp)
                throws InvalidSettingsException {
            return new FuzzyBasisFunctionPredictorRow(pp);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Creator getCreator() {
        return new FuzzyCreator();
    }
    
}
