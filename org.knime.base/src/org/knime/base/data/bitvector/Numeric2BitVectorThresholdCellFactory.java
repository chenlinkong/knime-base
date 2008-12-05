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
 * ---------------------------------------------------------------------
 *
 * History
 *   18.01.2007 (dill): created
 */
package org.knime.base.data.bitvector;

import java.util.List;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.vector.bitvector.DenseBitVector;
import org.knime.core.data.vector.bitvector.DenseBitVectorCellFactory;

/**
 *
 * @author Fabian Dill, University of Konstanz
 */
public class Numeric2BitVectorThresholdCellFactory
    extends BitVectorCellFactory {



    private int m_totalNrOf0s;
    private int m_totalNrOf1s;
    private double m_threshold;
    
    private final List<Integer>m_columns;

    /**
     *
     * @param bitColSpec {@link DataColumnSpec} of the column containing the
     * bitvectors
     * @param threshold the threshold above which the bit is set
     * @param columns list of column indixes used to create bit vector from
     */
    public Numeric2BitVectorThresholdCellFactory(
            final DataColumnSpec bitColSpec,
            final double threshold, final List<Integer>columns) {
        super(bitColSpec);
        m_threshold = threshold;
        m_columns = columns;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfNotSetBits() {
        return m_totalNrOf0s;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumberOfSetBits() {
        return m_totalNrOf1s;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean wasSuccessful() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell getCell(final DataRow row) {
        incrementNrOfRows();
        DenseBitVector bitSet = new DenseBitVector(m_columns.size());
        for (int i = 0; i < m_columns.size(); i++) {
            DataCell cell = row.getCell(m_columns.get(i));
            if (cell.isMissing()) {
                m_totalNrOf0s++;
                continue;
            }
            double currValue = ((DoubleValue)cell).getDoubleValue();
                if (currValue >= m_threshold) {
                    bitSet.set(i);
                    m_totalNrOf1s++;
                } else {
                    m_totalNrOf0s++;
                }
        }
        DenseBitVectorCellFactory fact = new DenseBitVectorCellFactory(bitSet);
        return fact.createDataCell();
    }

}
