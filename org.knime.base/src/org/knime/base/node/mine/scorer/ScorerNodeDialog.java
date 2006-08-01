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
package org.knime.base.node.mine.scorer;

import java.awt.FlowLayout;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JComboBox;
import javax.swing.JPanel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;


/**
 * A dialog for the scorer to set the two table columns to score for.
 * 
 * @author Christoph Sieb, University of Konstanz
 * @author Thomas Gabriel, University of Konstanz
 */
final class ScorerNodeDialog extends NodeDialogPane {
    /*
     * The main panel in this view.
     */
    private final JPanel m_p;

    /*
     * The text field for the first column to compare The first column
     * represents the real classes of the data
     */
    private final JComboBox m_firstColumns;

    /*
     * The text field for the second column to compare The second column
     * represents the predicted classes of the data
     */
    private final JComboBox m_secondColumns;

    /**
     * Creates a new {@link NodeDialogPane} for scoring in order to set the two
     * columns to compare.
     */
    ScorerNodeDialog() {
        super();

        m_p = new JPanel();
        m_p.setLayout(new BoxLayout(m_p, BoxLayout.Y_AXIS));

        m_firstColumns = new JComboBox();
        m_secondColumns = new JComboBox();

        JPanel firstColumnPanel = new JPanel(new GridLayout(1, 1));
        firstColumnPanel.setBorder(BorderFactory
                .createTitledBorder("First Column"));
        JPanel flowLayout = new JPanel(new FlowLayout());
        flowLayout.add(m_firstColumns);
        firstColumnPanel.add(flowLayout);

        JPanel secondColumnPanel = new JPanel(new GridLayout(1, 1));
        secondColumnPanel.setBorder(BorderFactory
                .createTitledBorder("Second Column"));
        flowLayout = new JPanel(new FlowLayout());
        flowLayout.add(m_secondColumns);
        secondColumnPanel.add(flowLayout);

        m_p.add(firstColumnPanel);

        m_p.add(secondColumnPanel);

        super.addTab("Scorer", m_p);
    } // ScorerNodeDialog(NodeModel)

    /**
     * Fills the two combo boxes with all column names retrieved from the input
     * table spec. The second and last column will be selected by default unless
     * the settings object contains others.
     * 
     * @see NodeDialogPane#loadSettingsFrom(NodeSettingsRO, DataTableSpec[])
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        assert (settings != null && specs != null);

        m_firstColumns.removeAllItems();
        m_secondColumns.removeAllItems();

        DataTableSpec spec = specs[ScorerNodeModel.INPORT];

        if ((spec == null) || (spec.getNumColumns() < 2)) {
            throw new NotConfigurableException("Scorer needs an input table "
                    + "with at least two columns");
        }
        if (spec != null) {

            int numCols = spec.getNumColumns();
            for (int i = 0; i < numCols; i++) {
                String c = spec.getColumnSpec(i).getName();
                m_firstColumns.addItem(c);
                m_secondColumns.addItem(c);
            }
            // if at least two columns available
            String col2 = (numCols > 0) ? spec.getColumnSpec(numCols - 1)
                    .getName() : null;
            String col1 = (numCols > 1) ? spec.getColumnSpec(numCols - 2)
                    .getName() : col2;
            col1 = settings.getString(ScorerNodeModel.FIRST_COMP_ID, col1);
            col2 = settings.getString(ScorerNodeModel.SECOND_COMP_ID, col2);
            m_firstColumns.setSelectedItem(col1);
            m_secondColumns.setSelectedItem(col2);
        }
    }

    /**
     * Sets the selected columns inside the {@link ScorerNodeModel}.
     * 
     * @param settings the object to write the settings into
     * @throws InvalidSettingsException if the column selection is invalid
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        assert (settings != null);

        String firstColumn = (String)m_firstColumns.getSelectedItem();
        String secondColumn = (String)m_secondColumns.getSelectedItem();

        if ((firstColumn == null) || (secondColumn == null)) {
            throw new InvalidSettingsException("Select two valid column names "
                    + "from the lists (or press cancel).");
        }
        if (m_firstColumns.getItemCount() > 1
                && firstColumn.equals(secondColumn)) {
            throw new InvalidSettingsException(
                    "First and second column cannot be the same.");
        }
        settings.addString(ScorerNodeModel.FIRST_COMP_ID, firstColumn);
        settings.addString(ScorerNodeModel.SECOND_COMP_ID, secondColumn);
    }
}
