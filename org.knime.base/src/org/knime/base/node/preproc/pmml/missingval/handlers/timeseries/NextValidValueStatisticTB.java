/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   18.12.2014 (Alexander): created
 */
package org.knime.base.node.preproc.pmml.missingval.handlers.timeseries;

import java.util.Iterator;

import org.knime.base.node.preproc.pmml.missingval.handlers.timeseries.MappingTableInterpolationStatistic.MappingTableIterator;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.def.DefaultRow;

/**
 * Table based statistic that finds for each missing value the next valid one.
 * @author Alexander Fillbrunn
 */
public class NextValidValueStatisticTB extends MappingStatistic {

    private DataContainer m_nextCells;
    private int m_numMissing = 0;
    private int m_counter = 0;
    private DataTable m_table;
    private String m_columnName;
    private int m_index = -1;

    /**
     * Constructor for NextValidValueStatistic.
     * @param clazz the class of the data value this statistic can be used for
     * @param column the column for which this statistic is calculated
     */
    public NextValidValueStatisticTB(final Class<? extends DataValue> clazz, final String column) {
        super(clazz, column);
        m_columnName = column;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init(final DataTableSpec spec, final int amountOfColumns) {
        m_index = spec.findColumnIndex(m_columnName);
        m_nextCells = new DataContainer(new DataTableSpec(spec.getColumnSpec(m_index)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void consumeRow(final DataRow dataRow) {
        DataCell cell = dataRow.getCell(m_index);
        if (cell.isMissing()) {
            m_numMissing++;
        } else {
            for (int i = 0; i < m_numMissing; i++) {
                m_nextCells.addRowToTable(new DefaultRow(new RowKey(Integer.toString(m_counter++)), cell));
            }
            m_numMissing = 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String afterEvaluation() {
        // All remaining enqueued cells have no next value and stay missing
        for (int i = 0; i < m_numMissing; i++) {
            m_nextCells.addRowToTable(new DefaultRow(new RowKey(Integer.toString(m_counter++)),
                                        DataType.getMissingCell()));
        }

        m_nextCells.close();
        m_table = m_nextCells.getTable();
        return super.afterEvaluation();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<DataCell> iterator() {
        return new MappingTableIterator(m_table);
    }
}
