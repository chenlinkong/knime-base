/*
 * --------------------------------------------------------------------- *
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2006
 * University of Konstanz, Germany.
 * Chair for Bioinformatics and Information Mining
 * Prof. Dr. Michael R. Berthold
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any quesions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * --------------------------------------------------------------------- *
 */
package org.knime.base.node.mine.bfn;

import java.util.Map;

import org.knime.base.data.append.column.AppendedColumnRow;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowIterator;

/**
 * Class wraps a row iterator in order to exents the given
 * {@link org.knime.core.data.DataRow} elements by on cell (resp. column
 * for the {@link org.knime.core.data.DataTable}).
 * 
 * @author Thomas Gabriel, University of Konstanz
 */
final class BasisFunctionPredictorRowIterator extends RowIterator {
    /*
     * The row iterator of the input data.
     */
    private final RowIterator m_rowIt;

    /*
     * Keeps the row key to class label.
     */
    private final Map<DataCell, DataCell> m_map;

    /**
     * Creates a new row iterator used for the basisfunction predictor node
     * which extends the input rows by a new, additional class label column.
     * 
     * @param rowIt the row iterator of the input data
     * @param map maps the row keys to class labels
     */
    BasisFunctionPredictorRowIterator(final RowIterator rowIt,
            final Map<DataCell, DataCell> map) {
        assert (rowIt != null);
        m_rowIt = rowIt;
        assert (map != null);
        m_map = map;
    }

    /**
     * @see org.knime.core.data.RowIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return m_rowIt.hasNext();
    }

    /**
     * @see org.knime.core.data.RowIterator#next()
     */
    @Override
    public DataRow next() {
        DataRow row = m_rowIt.next();
        DataCell classInfo = m_map.get(row.getKey().getId());
        return new AppendedColumnRow(row, classInfo);
    }
}
