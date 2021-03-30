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
 *   Aug 3, 2020 (Timmo Waller-Ehrat, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.utility.nodes.deletepaths.table;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.filehandling.utility.nodes.deletepaths.AbstractDeleteFilesAndFoldersNodeConfig;

/**
 * Configuration of the "Delete Files/Folders (Table input)" node.
 *
 * @author Lars Schweikardt, KNIME GmbH, Konstanz, Germany
 */
final class DeleteFilesAndFoldersTableNodeConfig extends AbstractDeleteFilesAndFoldersNodeConfig {

    private static final String CFG_COLUMN_SELECTION = "column_selection";

    private static final String CFG_FAIL_IF_FILE_NOT_EXIST = "fail_if_file_does_not_exist";

    private final SettingsModelString m_columnSelection;

    private final SettingsModelBoolean m_failIfFileDoesNotExist;

    DeleteFilesAndFoldersTableNodeConfig() {
        super();
        m_columnSelection = new SettingsModelString(CFG_COLUMN_SELECTION, null);
        m_failIfFileDoesNotExist = new SettingsModelBoolean(CFG_FAIL_IF_FILE_NOT_EXIST, true);
    }

    SettingsModelString getColumnSelection() {
        return m_columnSelection;
    }

    @Override
    protected boolean failIfFileDoesNotExist() {
        return getFailIfFileNotExistsModel().getBooleanValue();
    }

    SettingsModelBoolean getFailIfFileNotExistsModel() {
        return m_failIfFileDoesNotExist;
    }

    @Override
    protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.loadSettingsForModel(settings);
        m_columnSelection.loadSettingsFrom(settings);
        m_failIfFileDoesNotExist.loadSettingsFrom(settings);
    }

    @Override
    protected void saveSettingsForModel(final NodeSettingsWO settings) {
        super.saveSettingsForModel(settings);
        m_columnSelection.saveSettingsTo(settings);
        m_failIfFileDoesNotExist.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        super.validateSettingsForModel(settings);
        m_columnSelection.validateSettings(settings);
        m_failIfFileDoesNotExist.validateSettings(settings);
    }
}
