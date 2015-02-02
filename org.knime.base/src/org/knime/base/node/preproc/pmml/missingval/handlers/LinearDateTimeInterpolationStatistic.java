/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package org.knime.base.node.preproc.pmml.missingval.handlers;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.date.DateAndTimeValue;

/**
 * A statistic that calculates for each missing cell the linear interpolation
 * between the previous and next valid cell.
 * @author Alexander Fillbrunn
 */
public class LinearDateTimeInterpolationStatistic extends InterpolationStatistic {

    private int m_numMissing = 0;

    /**
     * Constructor for NextValidValueStatistic.
     * @param column the column for which this statistic is calculated
     */
    public LinearDateTimeInterpolationStatistic(final String column) {
        super(DateAndTimeValue.class, column);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void consumeRow(final DataRow dataRow) {
        DataCell cell = dataRow.getCell(getColumnIndex());
        if (cell.isMissing()) {
            addToQueue(dataRow.getKey());
            m_numMissing++;
        } else {
            DateAndTimeValue val = (DateAndTimeValue)cell;
            DataTable table = closeQueued();
            int count = 1;
            for (DataRow row : table) {
                DataCell res;
                if (getPrevious().isMissing()) {
                    res = cell;
                } else {

                    DateAndTimeValue prevVal = (DateAndTimeValue)getPrevious();

                    boolean hasDate = val.hasDate() | prevVal.hasDate();
                    boolean hasTime = val.hasTime() | prevVal.hasTime();
                    boolean hasMilis = val.hasMillis() | prevVal.hasMillis();

                    long prev = prevVal.getUTCTimeInMillis();
                    long next = val.getUTCTimeInMillis();
                    long lin = Math.round(prev + 1.0 * (count++) / (1.0 * (m_numMissing + 1)) * (next - prev));
                    res = new DateAndTimeCell(lin, hasDate, hasTime, hasMilis);
                }
                addMapping(row.getKey(), res);
            }
            resetQueue(cell);
            m_numMissing = 0;
        }
    }
}
