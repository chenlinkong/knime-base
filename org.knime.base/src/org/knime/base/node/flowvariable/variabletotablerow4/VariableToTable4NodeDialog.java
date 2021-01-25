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
 *   May 1, 2008 (wiswedel): created
 */
package org.knime.base.node.flowvariable.variabletotablerow4;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.JPanel;

import org.knime.base.node.flowvariable.converter.variabletocell.VariableToCellConverterFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.filter.variable.FlowVariableFilterConfiguration;
import org.knime.core.node.util.filter.variable.FlowVariableFilterPanel;
import org.knime.core.node.util.filter.variable.VariableTypeFilter;
import org.knime.core.node.workflow.VariableType;

/**
 * Dialog for the "Variable To TableRow" node.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
final class VariableToTable4NodeDialog extends NodeDialogPane {

    private final FlowVariableFilterPanel m_filter;

    private final DialogComponentString m_rowID;

    VariableToTable4NodeDialog() {
        m_filter =
            new FlowVariableFilterPanel(new VariableTypeFilter(VariableToCellConverterFactory.getSupportedTypes()));
        m_rowID =
            new DialogComponentString(VariableToTable4NodeModel.createSettingsModelRowID(), "Row name  ", true, 13);
        addTab("Variable Selection", createPanel());
    }

    private JPanel createPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.weighty = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;

        panel.add(createRowIdPanel(), gbc);

        ++gbc.gridy;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;
        panel.add(createVarSelectionPanel(), gbc);

        return panel;
    }

    private Component createRowIdPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.NONE;

        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "RowID"));
        panel.add(m_rowID.getComponentPanel(), gbc);

        ++gbc.gridy;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(Box.createVerticalBox(), gbc);

        return panel;
    }

    private Component createVarSelectionPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.BOTH;

        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Variable Selection"));
        panel.add(m_filter, gbc);

        return panel;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_rowID.loadSettingsFrom(settings, specs);
        final FlowVariableFilterConfiguration config =
            new FlowVariableFilterConfiguration(AbstractVariableToTableNodeModel.CFG_KEY_FILTER);
        final VariableType<?>[] types = VariableToCellConverterFactory.getSupportedTypes();
        config.loadConfigurationInDialog(settings, getAvailableFlowVariables(types));
        m_filter.loadConfiguration(config, getAvailableFlowVariables(types));
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_rowID.saveSettingsTo(settings);
        final FlowVariableFilterConfiguration config =
            new FlowVariableFilterConfiguration(AbstractVariableToTableNodeModel.CFG_KEY_FILTER);
        m_filter.saveConfiguration(config);
        config.saveConfiguration(settings);
    }

}
