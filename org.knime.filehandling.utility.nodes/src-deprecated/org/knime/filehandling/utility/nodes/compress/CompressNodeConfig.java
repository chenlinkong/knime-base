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
 *   27 Aug 2020 (Timmo Waller-Ehrat, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.utility.nodes.compress;

import java.util.EnumSet;

import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;

/**
 * Node Config for the "Compress Files/Folder" node
 *
 * @author Timmo Waller-Ehrat, KNIME GmbH, Konstanz, Germany
 * @deprecated since 4.3.3
 */
@Deprecated
final class CompressNodeConfig {

    static final String INVALID_EXTENSION_ERROR =
        "Invalid destination file extension. Please find the valid extensions in the node description.";

    private static final String CFG_INPUT_LOCATION = "source_location";

    private static final String CFG_OUTPUT_LOCATION = "destination_location";

    private static final String CFG_INCLUDE_SELECTED_FOLDER = "include_selected_source_folder";

    private static final String CFG_FLATTEN_HIERARCHY = "flatten_hierarchy";

    private static final String CFG_COMPRESSION = "compression";

    private final SettingsModelReaderFileChooser m_inputLocationChooserModel;

    private final SettingsModelWriterFileChooser m_destinationFileChooserModel;

    private final SettingsModelString m_compressionModel;

    private boolean m_flattenHierarchy;

    private boolean m_includeFolder;

    static final String BZ2_EXTENSION = "bz2";

    static final String GZ_EXTENSION = "gz";

    static final String[] COMPRESSIONS = new String[]{//
        ArchiveStreamFactory.ZIP, //
        ArchiveStreamFactory.JAR, //
        ArchiveStreamFactory.TAR, //
        ArchiveStreamFactory.TAR + "." + GZ_EXTENSION, //
        ArchiveStreamFactory.TAR + "." + BZ2_EXTENSION, //
        ArchiveStreamFactory.CPIO};

    /** The default compression is zip. */
    private static final String DEFAULT_COMPRESSION = COMPRESSIONS[0];

    /**
     * Constructor
     *
     * @param portsConfig {@link PortsConfiguration} of the node
     */
    CompressNodeConfig(final PortsConfiguration portsConfig) {
        m_inputLocationChooserModel = new SettingsModelReaderFileChooser(CFG_INPUT_LOCATION, portsConfig,
            CompressNodeFactory.CONNECTION_INPUT_FILE_PORT_GRP_NAME, FilterMode.FILE);

        m_destinationFileChooserModel = new SettingsModelWriterFileChooser(CFG_OUTPUT_LOCATION, portsConfig,
            CompressNodeFactory.CONNECTION_OUTPUT_DIR_PORT_GRP_NAME, FilterMode.FILE, FileOverwritePolicy.FAIL,
            EnumSet.of(FileOverwritePolicy.FAIL, FileOverwritePolicy.OVERWRITE), COMPRESSIONS);
        m_compressionModel = new SettingsModelString(CFG_COMPRESSION, DEFAULT_COMPRESSION);
        m_includeFolder = false;
        m_flattenHierarchy = false;
    }

    void loadSettingsForDialog(final NodeSettingsRO settings) {
        includeSelectedFolder(settings.getBoolean(CFG_INCLUDE_SELECTED_FOLDER, false));
        flattenHierarchy(settings.getBoolean(CFG_FLATTEN_HIERARCHY, false));
    }

    void saveSettingsForDialog(final NodeSettingsWO settings) {
        saveNonSettingModelParameters(settings);
    }

    private void saveIncludeParentFolder(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_INCLUDE_SELECTED_FOLDER, includeSourceFolder());
    }

    private void saveFlattenHierarchy(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_FLATTEN_HIERARCHY, flattenHierarchy());
    }

    void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_inputLocationChooserModel.validateSettings(settings);
        m_destinationFileChooserModel.validateSettings(settings);
        m_compressionModel.validateSettings(settings);
        settings.getBoolean(CFG_INCLUDE_SELECTED_FOLDER);
        settings.getBoolean(CFG_FLATTEN_HIERARCHY);
    }

    void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettingsForDialog(settings);
        m_inputLocationChooserModel.loadSettingsFrom(settings);
        m_destinationFileChooserModel.loadSettingsFrom(settings);
        m_compressionModel.loadSettingsFrom(settings);
    }

    void saveSettingsForModel(final NodeSettingsWO settings) {
        saveNonSettingModelParameters(settings);

        m_inputLocationChooserModel.saveSettingsTo(settings);
        m_destinationFileChooserModel.saveSettingsTo(settings);
        m_compressionModel.saveSettingsTo(settings);
    }

    private void saveNonSettingModelParameters(final NodeSettingsWO settings) {
        saveIncludeParentFolder(settings);
        saveFlattenHierarchy(settings);
    }

    /**
     * Returns the {@link SettingsModelWriterFileChooser} used to select where to save the archive file.
     *
     * @return the {@link SettingsModelWriterFileChooser} used to select a directory
     */
    SettingsModelWriterFileChooser getTargetFileChooserModel() {
        return m_destinationFileChooserModel;
    }

    /**
     * Returns the {@link SettingsModelReaderFileChooser} used to select a file, folder or files in folder which should
     * get compressed.
     *
     * @return the {@link SettingsModelReaderFileChooser} used to select a directory
     */
    SettingsModelReaderFileChooser getInputLocationChooserModel() {
        return m_inputLocationChooserModel;
    }

    SettingsModelString getCompressionModel() {
        return m_compressionModel;
    }

    boolean includeSourceFolder() {
        return m_includeFolder;
    }

    void includeSelectedFolder(final boolean include) {
        m_includeFolder = include;
    }

    /**
     * Returns the flag deciding whether or not to flatten the hierarchy during compression.
     *
     * @return {code true} if the hierarchy has to be flattened during compression and {@code false} otherwise
     */
    boolean flattenHierarchy() {
        return m_flattenHierarchy;
    }

    /**
     * Sets the flag indicating whether or not to flatten the hierarchy during compression.
     *
     * @param flattenHierarchy {@code true} if the hierarchy has to be flattened during compression and {@code false}
     *            otherwise
     */
    void flattenHierarchy(final boolean flattenHierarchy) {
        m_flattenHierarchy = flattenHierarchy;
    }

}
