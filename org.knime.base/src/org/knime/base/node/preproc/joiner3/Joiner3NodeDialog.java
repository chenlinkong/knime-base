/*
 * ------------------------------------------------------------------------
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
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
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
 *   27.07.2007 (thor): created
 */
package org.knime.base.node.preproc.joiner3;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.util.Arrays;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.event.ChangeListener;

import org.knime.base.node.preproc.joiner3.Joiner3Settings.ColumnNameDisambiguation;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.CompositionMode;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.JoinMode;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.join.JoinSpecification.OutputRowOrder;
import org.knime.core.data.join.JoinTableSettings.JoinColumn;
import org.knime.core.data.join.JoinTableSettings.SpecialJoinColumn;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.ColumnPairsSelectionPanel;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;


/**
 * This is the dialog for the joiner node.
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 */
class Joiner3NodeDialog extends NodeDialogPane {

    private final Joiner3Settings m_settings = new Joiner3Settings();

    // join mode
    private final JCheckBox m_includeMatchingRows = new JCheckBox("Include matching rows");
    private final JCheckBox m_includeLeftUnmatchedRows = new JCheckBox("Include left unmatched rows");
    private final JCheckBox m_includeRightUnmatchedRows = new JCheckBox("Include right unmatched rows");

    // join columns
//    private final JRadioButton m_matchAllButton = new JRadioButton("Match all of the following");
//    private final JRadioButton m_matchAnyButton = new JRadioButton("Match any of the following");
    private ColumnPairsSelectionPanel m_columnPairs;

    // output
    private final JCheckBox m_mergeJoinColumns = new JCheckBox("Merge join columns");
    private final JCheckBox m_outputUnmatchedRowsToSeparatePorts =
        new JCheckBox("Output unmatched rows to separate ports");

    // row keys
    private final JRadioButton m_concatOrgRowKeysWithSeparator =
        new JRadioButton("Concatenate original row keys with separator:");
    private final JTextField m_rowKeySeparator = new JTextField();
    private final JRadioButton m_assignNewRowKeys = new JRadioButton("Assign new row keys sequentially");

    // column selection
    private DataColumnSpecFilterPanel m_leftFilterPanel;
    private DataColumnSpecFilterPanel m_rightFilterPanel;
    private final JRadioButton m_dontExecute = new JRadioButton("Don't execute");
    private final JRadioButton m_appendSuffixAutomatic = new JRadioButton("Append suffix (automatic)");
    private final JRadioButton m_appendSuffix = new JRadioButton("Append custom suffix:");
    private final JTextField m_suffix = new JTextField();

    // performance
    private final JRadioButton m_outputOrderArbitrary = new JRadioButton(OutputRowOrder.ARBITRARY.toString());
    private final JRadioButton m_outputOrderDeterministic =
        new JRadioButton(OutputRowOrder.DETERMINISTIC.toString());
    private final JRadioButton m_outputOrderLeftRight =
        new JRadioButton(OutputRowOrder.LEFT_RIGHT.toString());
    private final JTextField m_maxOpenFiles = new JTextField(10);
    private final JCheckBox m_hilitingEnabled = new JCheckBox("Hiliting enabled");

    /**
     * Creates a new dialog for the joiner node.
     * @param settings
     */
    Joiner3NodeDialog() {
        addTab("Joiner Settings", createJoinerSettingsTab());
        addTab("Column Selection", createColumnSelectionTab());
        addTab("Performance", createPerformanceTab());
    }

    private JPanel createJoinerSettingsTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weighty = 0;
        p.add(createJoinModePanel(), c);

        c.gridy++;
        p.add(createJoinColumnsPanel(), c);

        c.gridy++;
        p.add(createOutputPanel(), c);

        c.gridy++;
        p.add(createRowKeysPanel(), c);

        c.gridy++;
        c.weightx = 100;
        c.weighty = 100;
        c.fill = GridBagConstraints.BOTH;
        p.add(new JPanel(), c);

        return p;
    }

    private JPanel createJoinModePanel() {
        JPanel p = new JPanel(new BorderLayout());

        JPanel left = new JPanel(new GridLayout(3,1));
        left.add(m_includeMatchingRows);
        left.add(m_includeLeftUnmatchedRows);
        left.add(m_includeRightUnmatchedRows);

        JPanel right = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        JPanel joinModePanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 10));
        JLabel joinModeName = new JLabel("");
        joinModePanel.add(joinModeName);
        right.add(joinModePanel, c);
        ChangeListener joinModeListener = e -> {
            String determiner = "a";
            switch (getJoinMode()) {
                case EmptyJoin:
                    joinModePanel.setBackground(new Color(0xff8596));
                    joinModeName.setText("No output rows");
                    break;
                case InnerJoin:
                    determiner = "an";
                default:
                    joinModePanel.setBackground(Color.white);
                    joinModeName.setText(String.format(
                        "<html><body style=\"text-align: center\">This corresponds to %s<br>%s</body></html>",
                        determiner, getJoinMode().toString().toLowerCase()));
            }
        };
        m_includeMatchingRows.addChangeListener(joinModeListener);
        m_includeLeftUnmatchedRows.addChangeListener(joinModeListener);
        m_includeRightUnmatchedRows.addChangeListener(joinModeListener);

        p.setBorder(BorderFactory.createTitledBorder("Join Mode"));
        p.add(left, BorderLayout.WEST);
        p.add(right, BorderLayout.CENTER);
        return p;
    }

    private JoinMode getJoinMode() {
        return Arrays.stream(JoinMode.values())
            .filter(mode -> mode.isIncludeMatchingRows() == m_includeMatchingRows.isSelected()
                && mode.isIncludeLeftUnmatchedRows() == m_includeLeftUnmatchedRows.isSelected()
                && mode.isIncludeRightUnmatchedRows() == m_includeRightUnmatchedRows.isSelected())
            .findFirst().orElseThrow(() -> new IllegalStateException("Unknown join mode selected in dialog."));
    }

    @SuppressWarnings("serial")
    private JPanel createJoinColumnsPanel() {
        JPanel p = new JPanel(new GridBagLayout());

        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);

        c.gridx = 0;
        c.gridy = 0;

//        JPanel matchButtonPanel = new JPanel(new FlowLayout());
//        matchButtonPanel.add(m_matchAllButton);
//        matchButtonPanel.add(m_matchAnyButton);
//        p.add(matchButtonPanel, c);
//
//        ButtonGroup group = new ButtonGroup();
//        group.add(m_matchAllButton);
//        group.add(m_matchAnyButton);

        c.gridx = 0;
        c.gridy++;

        c.gridwidth = 3;
        c.weightx = 1;
        c.weighty = 1;

        m_columnPairs = new ColumnPairsSelectionPanel(true) {
            @Override
            protected void initComboBox(final DataTableSpec spec,
                @SuppressWarnings("rawtypes") final JComboBox comboBox, final String selected) {
                super.initComboBox(spec, comboBox, selected);
                // the first entry is the row id, set as default.
                if (selected == null) {
                    comboBox.setSelectedIndex(0);
                }
            }
        };
        JScrollPane scrollPane = new JScrollPane(m_columnPairs);
        m_columnPairs.setBackground(Color.white);
        Component header = m_columnPairs.getHeaderView();
        header.setPreferredSize(new Dimension(300, 20));
        scrollPane.setColumnHeaderView(header);
        scrollPane.setPreferredSize(new Dimension(300, 200));
        scrollPane.setMinimumSize(new Dimension(300, 100));

        p.add(scrollPane, c);
        p.setBorder(BorderFactory.createTitledBorder("Join Columns"));

        return p;

    }

    private JPanel createOutputPanel() {
        JPanel p = new JPanel(new GridLayout(2, 1));
        p.add(m_mergeJoinColumns);
        p.add(m_outputUnmatchedRowsToSeparatePorts);

        p.setBorder(BorderFactory.createTitledBorder("Output"));
        return p;
    }

    private JPanel createRowKeysPanel() {
        JPanel p = new JPanel(new GridLayout(2, 1));
        JPanel concatRowKeys = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        concatRowKeys.add(m_concatOrgRowKeysWithSeparator);
        m_concatOrgRowKeysWithSeparator
            .addChangeListener(l -> m_rowKeySeparator.setEnabled(m_concatOrgRowKeysWithSeparator.isSelected()));
        concatRowKeys.add(m_rowKeySeparator);
        p.add(concatRowKeys);

        p.add(m_assignNewRowKeys);

        ButtonGroup bg = new ButtonGroup();
        bg.add(m_concatOrgRowKeysWithSeparator);
        bg.add(m_assignNewRowKeys);

        m_rowKeySeparator.setPreferredSize(new Dimension(50,
                m_rowKeySeparator.getPreferredSize().height));

        p.setBorder(BorderFactory.createTitledBorder("Row Keys"));
        return p;
    }

    private JComponent createColumnSelectionTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.gridx = 0;
        c.gridy = 0;

        c.weightx = 1;
        c.gridwidth = 1;
        m_leftFilterPanel = new DataColumnSpecFilterPanel();
        m_leftFilterPanel.setBorder(
                BorderFactory.createTitledBorder("Top Input ('left' table)"));
        p.add(m_leftFilterPanel, c);
        c.gridy++;
        m_rightFilterPanel = new DataColumnSpecFilterPanel();
        m_rightFilterPanel.setBorder(
                BorderFactory.createTitledBorder("Bottom Input ('right' table)"));
        p.add(m_rightFilterPanel, c);
        c.gridy++;
        p.add(createDuplicateColumnNamesPanel(), c);
        return new JScrollPane(p);
    }

    private JPanel createDuplicateColumnNamesPanel() {
        JPanel p = new JPanel(new GridLayout(3, 1));
        p.add(m_dontExecute);
        p.add(m_appendSuffixAutomatic);
        JPanel suffix = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        suffix.add(m_appendSuffix);

        m_suffix.setPreferredSize(new Dimension(100,
                m_suffix.getPreferredSize().height));
        suffix.add(m_suffix);
        p.add(suffix);

        m_appendSuffix.addChangeListener(e -> m_suffix.setEnabled(m_appendSuffix.isSelected()));

        ButtonGroup duplicateColGroup = new ButtonGroup();
        duplicateColGroup.add(m_dontExecute);
        duplicateColGroup.add(m_appendSuffixAutomatic);
        duplicateColGroup.add(m_appendSuffix);

        p.setBorder(BorderFactory.createTitledBorder(
                "Duplicate column names"));
        return p;
    }

    private JComponent createPerformanceTab() {
        JPanel p = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTHWEST;
        c.insets = new Insets(2, 2, 2, 2);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        c.weighty = 0;
        p.add(createOutputOrderPanel(), c);

        c.gridy++;
        JPanel misc = new JPanel(new GridLayout(2, 1));
        JPanel maxFiles = new JPanel(new FlowLayout(FlowLayout.LEFT));
        maxFiles.add(new JLabel("Maximum number of open files "));
        maxFiles.add(m_maxOpenFiles);
        misc.add(maxFiles);
        misc.add(m_hilitingEnabled);
        misc.setBorder(BorderFactory.createTitledBorder("Miscellaneous"));
        p.add(misc, c);

        c.gridy++;
        c.weightx = 100;
        c.weighty = 100;
        c.fill = GridBagConstraints.BOTH;
        p.add(new JPanel(), c);

        return p;
    }

    private JPanel createOutputOrderPanel() {
        JPanel p = new JPanel(new GridLayout(3,1));
        p.add(m_outputOrderArbitrary);
//        p.add(m_outputOrderDeterministic);
        p.add(m_outputOrderLeftRight);

        ButtonGroup bg = new ButtonGroup();
        bg.add(m_outputOrderArbitrary);
//        bg.add(m_outputOrderDeterministic);
        bg.add(m_outputOrderLeftRight);

        p.setBorder(BorderFactory.createTitledBorder(
                "Output order"));
        return p;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final DataTableSpec[] specs) throws NotConfigurableException {
        m_settings.loadSettingsForDialog(settings, specs);

        // join mode
        JoinMode joinMode = m_settings.getJoinMode();
        m_includeMatchingRows.setSelected(joinMode.isIncludeMatchingRows());
        m_includeLeftUnmatchedRows.setSelected(joinMode.isIncludeLeftUnmatchedRows());
        m_includeRightUnmatchedRows.setSelected(joinMode.isIncludeRightUnmatchedRows());

//        m_matchAllButton.setSelected(m_settings.getCompositionMode().equals(CompositionMode.MatchAll));
//        m_matchAnyButton.setSelected(m_settings.getCompositionMode().equals(CompositionMode.MatchAny));

        // join columns
        String[] leftSelected = joinClauseToColumnPairs(m_settings.getLeftJoinColumns());
        String[] rightSelected = joinClauseToColumnPairs(m_settings.getRightJoinColumns());
        m_columnPairs.updateData(specs, leftSelected, rightSelected);

        // output
        m_mergeJoinColumns.setSelected(m_settings.isMergeJoinColumns());
        m_outputUnmatchedRowsToSeparatePorts.setSelected(m_settings.isOutputUnmatchedRowsToSeparateOutputPort());

        // row keys
        m_rowKeySeparator.setText(m_settings.getRowKeySeparator());
        m_concatOrgRowKeysWithSeparator.setSelected(!m_settings.isAssignNewRowKeys());
        m_assignNewRowKeys.setSelected(m_settings.isAssignNewRowKeys());

        // column selection
        m_leftFilterPanel.loadConfiguration(m_settings.getLeftColumnSelectionConfig(), specs[0]);
        m_rightFilterPanel.loadConfiguration(m_settings.getRightColumnSelectionConfig(), specs[1]);
        m_dontExecute.setSelected(m_settings.getDuplicateHandling() == ColumnNameDisambiguation.DO_NOT_EXECUTE);
        m_appendSuffixAutomatic
            .setSelected(m_settings.getDuplicateHandling() == ColumnNameDisambiguation.APPEND_SUFFIX_AUTOMATIC);
        m_appendSuffix.setSelected(m_settings.getDuplicateHandling() == ColumnNameDisambiguation.APPEND_SUFFIX);
        m_suffix.setText(m_settings.getDuplicateColumnSuffix());
        m_suffix.setEnabled(m_settings.getDuplicateHandling() == ColumnNameDisambiguation.APPEND_SUFFIX);

        // performance
        m_outputOrderArbitrary.setSelected(false);
        m_outputOrderDeterministic.setSelected(false);
        m_outputOrderLeftRight.setSelected(false);
        switch (m_settings.getOutputRowOrder()) {
            case ARBITRARY:
                m_outputOrderArbitrary.setSelected(true);
                break;
            case DETERMINISTIC:
                m_outputOrderDeterministic.setSelected(true);
                break;
            case LEFT_RIGHT:
                m_outputOrderLeftRight.setSelected(true);
        }
        m_maxOpenFiles.setText(Integer.toString(m_settings.getMaxOpenFiles()));
        m_hilitingEnabled.setSelected(m_settings.isHilitingEnabled());
    }

    /**
     * Transform the {@link JoinColumn}s used in the settings and the joiner API back to {@link String}s, as understood
     * by the {@link ColumnPairsSelectionPanel}.
     *
     * @param joinColumns as stored in {@link Joiner3Settings#getLeftJoinColumns()} or
     *            {@link Joiner3Settings#getRightJoinColumns()}
     * @return string representation as understood by
     *         {@link ColumnPairsSelectionPanel#updateData(DataTableSpec[], String[], String[])}
     * @see #columnPairsToJoinClauses(Object[])
     */
    private String[] joinClauseToColumnPairs(final JoinColumn[] joinColumns) {
        return Arrays.stream(joinColumns)
            .map(jc -> jc.isColumn() ? jc.toColumnName() : m_columnPairs.getRowKeyIdentifier()).toArray(String[]::new);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {

        // join mode
        m_settings.setJoinMode(getJoinMode());

        // join columns
        m_settings.setCompositionMode(CompositionMode.MatchAll);

        m_settings.setLeftJoinColumns(columnPairsToJoinClauses(m_columnPairs.getLeftSelectedItems()));
        m_settings.setRightJoinColumns(columnPairsToJoinClauses(m_columnPairs.getRightSelectedItems()));

        // output
        m_settings.setMergeJoinColumns(m_mergeJoinColumns.isSelected());
        m_settings.setOutputUnmatchedRowsToSeparateOutputPort(m_outputUnmatchedRowsToSeparatePorts.isSelected());

        // row keys
        m_settings.setAssignNewRowKeys(m_assignNewRowKeys.isSelected());
        m_settings.setRowKeySeparator(m_rowKeySeparator.getText());

        // column selection
        if (m_dontExecute.isSelected()) {
            m_settings.setDuplicateHandling(ColumnNameDisambiguation.DO_NOT_EXECUTE);
        } else if (m_appendSuffixAutomatic.isSelected()) {
            m_settings.setDuplicateHandling(ColumnNameDisambiguation.APPEND_SUFFIX_AUTOMATIC);
        } else {
            String suffix = m_suffix.getText();
            if (suffix.isEmpty()) {
                throw new InvalidSettingsException(
                    "Append suffix is selected to disambiguate column names, but no suffix is provided.");
            }
            m_settings.setDuplicateHandling(ColumnNameDisambiguation.APPEND_SUFFIX);
            m_settings.setDuplicateColumnSuffix(suffix);
        }
        m_leftFilterPanel.saveConfiguration(m_settings.getLeftColumnSelectionConfig());
        m_rightFilterPanel.saveConfiguration(m_settings.getRightColumnSelectionConfig());

        // performance
        if (m_outputOrderArbitrary.isSelected()) {
            m_settings.setOutputRowOrder(OutputRowOrder.ARBITRARY);
        } else if (m_outputOrderDeterministic.isSelected()) {
            m_settings.setOutputRowOrder(OutputRowOrder.DETERMINISTIC);
        } else if (m_outputOrderLeftRight.isSelected()) {
            m_settings.setOutputRowOrder(OutputRowOrder.LEFT_RIGHT);
        }
        m_settings.setMaxOpenFiles(Integer.valueOf(m_maxOpenFiles.getText()));

        m_settings.validateSettings();
        m_settings.enabledHiliting(m_hilitingEnabled.isSelected());

        m_settings.saveSettings(settings);
    }

    /**
     * Converts the left/right hand sides entered in the {@link ColumnPairsSelectionPanel} to {@link JoinColumn} objects
     *
     * @param clauses as retrieved by {@link ColumnPairsSelectionPanel#getLeftSelectedItems()} or
     *            {@link ColumnPairsSelectionPanel#getRightSelectedItems()}
     * @throws InvalidSettingsException
     * @see {@link #joinClauseToColumnPairs(JoinColumn[])}
     */
    private JoinColumn[] columnPairsToJoinClauses(final Object[] clauses) throws InvalidSettingsException {
        JoinColumn[] result = new JoinColumn[clauses.length];
        for (int i = 0; i < clauses.length; i++) {
            if (clauses[i] == null) {
                throw new InvalidSettingsException(
                    "There are invalid  joining columns (highlighted with a red border).");
            }
            if (clauses[i] == m_columnPairs.getRowKeyIdentifier()) {
                result[i] = new JoinColumn(SpecialJoinColumn.ROW_KEY);
            } else {
                result[i] = new JoinColumn(((DataColumnSpec)clauses[i]).getName());
            }
        }
        return result;
    }

}