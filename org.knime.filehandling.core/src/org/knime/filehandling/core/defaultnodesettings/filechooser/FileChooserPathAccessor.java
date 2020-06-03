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
 *   May 26, 2020 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.defaultnodesettings.filechooser;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.FileSystemHelper;
import org.knime.filehandling.core.defaultnodesettings.ValidationUtils;
import org.knime.filehandling.core.defaultnodesettings.filesystemchooser.status.DefaultStatusMessage;
import org.knime.filehandling.core.defaultnodesettings.filesystemchooser.status.StatusMessage;
import org.knime.filehandling.core.defaultnodesettings.filesystemchooser.status.StatusMessage.MessageType;
import org.knime.filehandling.core.defaultnodesettings.filtermode.FileAndFolderFilter;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;

/**
 * Allows access to the {@link FSPath FSPaths} referred to by the {@link SettingsModelFileChooser3} provided in the constructor.
 * The paths are also validated and respective exceptions are thrown if the settings yield invalid paths.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class FileChooserPathAccessor implements ReadPathAccessor, WritePathAccessor {

    /**
     * The root location i.e. the location to start scanning the file tree from
     */
    private final FSLocation m_rootLocation;

    private final Optional<FSConnection> m_portObjectConnection;

    private final SettingsModelFileChooser3 m_settings;

    private FileFilterStatistic m_fileFilterStatistic;

    /**
     * The connection used to create the {@link FSFileSystem} used to create the {@link FSPath paths}.</br>
     * If m_rootLocation isn't empty, this will be initialized with m_rootLocation.get().
     */
    private FSConnection m_connection;

    private FSFileSystem<?> m_fileSystem;

    /**
     * Creates a new FileChooserAccessor for the provided {@link SettingsModelFileChooser3} and {@link FSConnection
     * connection} (if provided).</br>
     * The settings are not validated in this constructor but instead if {@link #getOutputPath(Consumer)} or
     * {@link #getPaths(Consumer)} are called.
     *
     * @param settings {@link SettingsModelFileChooser3} provided by the user
     * @param portObjectConnection connection retrieved from the file system port object (if the node has one)
     */
    public FileChooserPathAccessor(final SettingsModelFileChooser3 settings,
        final Optional<FSConnection> portObjectConnection) {
        m_rootLocation = settings.getLocation();
        m_portObjectConnection = portObjectConnection;
        m_settings = settings.createClone();
    }

    private FSConnection getConnection() {
        if (m_portObjectConnection.isPresent()) {
            // if we have a file system port, we always use the provided connection
            return m_portObjectConnection.get();
        } else {
            // otherwise we retrieve the connection from the location
            return FileSystemHelper.retrieveFSConnection(m_portObjectConnection, m_rootLocation)
                .orElseThrow(() -> new IllegalStateException("No connection available. Execute connector node."));
        }
    }

    private void initializeFileSystem() {
        if (m_connection == null) {
            m_connection = getConnection();
            m_fileSystem = m_connection.getFileSystem();
        }
    }

    /**
     * Retrieves the output/root {@link FSPath} specified in the settings provided to the constructor.</br>
     * Writer nodes should make use of this method.
     *
     * @param statusMessageConsumer used to communicating non-fatal erros and warnings
     * @return the output path
     * @throws InvalidSettingsException if the settings provided in the constructor are invalid
     */
    @Override
    public FSPath getOutputPath(final Consumer<StatusMessage> statusMessageConsumer)
        throws InvalidSettingsException {
        CheckUtils.checkArgumentNotNull(statusMessageConsumer, "The statusMessageConsumer must not be null.");

        initializeFileSystem();

        try {
            m_fileSystem.checkCompatibility(m_rootLocation);
        } catch (Exception ex) {
            statusMessageConsumer.accept(new DefaultStatusMessage(MessageType.ERROR, ex.getMessage()));
        }

        if (m_portObjectConnection.isPresent()) {
            // if present the port object fs always takes precedence
            return m_fileSystem.getPath(m_rootLocation);
        } else {
            return getPathFromConvenienceFs();
        }
    }

    private FSPath getPathFromConvenienceFs() throws InvalidSettingsException {
        switch (m_rootLocation.getFileSystemChoice()) {
            case CONNECTED_FS:
                throw new IllegalStateException("The file system is not connected.");
            case CUSTOM_URL_FS:
                return m_fileSystem.getPath(ValidationUtils.createAndValidateCustomURLLocation(m_rootLocation));
            case KNIME_FS:
                final FSPath path = m_fileSystem.getPath(m_rootLocation);
                ValidationUtils.validateKnimeFSPath(path);
                return path;
            case KNIME_MOUNTPOINT:
                return m_fileSystem.getPath(m_rootLocation);
            case LOCAL_FS:
                ValidationUtils.validateLocalFsAccess();
                return m_fileSystem.getPath(m_rootLocation);
            default:
                throw new IllegalStateException(
                    "Unsupported file system choice: " + m_rootLocation.getFileSystemChoice());
        }
    }

    /**
     * Retrieves the {@link Path paths} corresponding to the settings provided in the constructor.</br>
     * Reader nodes should make use of this method.
     *
     * @param statusMessageConsumer for communicating non-fatal errors and warnings
     * @return the list of paths corresponding to the settings
     * @throws IOException if an I/O problem occurs while listing the files
     * @throws InvalidSettingsException if the settings are invalid e.g. the root path is invalid
     */
    @Override
    public final List<FSPath> getPaths(final Consumer<StatusMessage> statusMessageConsumer)
        throws IOException, InvalidSettingsException {
        final FSPath rootPath = getOutputPath(statusMessageConsumer);

        final FilterMode filterMode = getFilterMode();
        if (filterMode == FilterMode.FILE || filterMode == FilterMode.FOLDER) {
            return handleSinglePath(rootPath);
        } else {
            return walkFileTree(rootPath);
        }
    }

    private List<FSPath> handleSinglePath(final FSPath rootPath) throws IOException, InvalidSettingsException {

        final BasicFileAttributes attr = Files.readAttributes(rootPath, BasicFileAttributes.class);
        final FilterMode filterMode = getFilterMode();
        if (filterMode == FilterMode.FILE) {
            CheckUtils.checkSetting(attr.isRegularFile(), "%s is not a regular file. Please specify a file.", rootPath);
            m_fileFilterStatistic = new FileFilterStatistic(0, 1, 0, 0);
        } else if (filterMode == FilterMode.FOLDER) {
            CheckUtils.checkSetting(attr.isDirectory(), "%s is not a folder. Please specify a folder.", rootPath);
            m_fileFilterStatistic = new FileFilterStatistic(0, 0, 0, 1);
        } else {
            throw new IllegalStateException("Unexpected filter mode in handleSingleCase: " + filterMode);
        }
        return Collections.singletonList(rootPath);
    }

    @Override
    public FileFilterStatistic getFileFilterStatistic() {
        CheckUtils.checkState(m_fileFilterStatistic != null, "No statistic available. Call getPaths() first.");
        return m_fileFilterStatistic;
    }

    private List<FSPath> walkFileTree(final Path rootPath) throws IOException {
        final FilterVisitor visitor = createVisitor();
        final boolean includeSubfolders = m_settings.getFilterModeModel().isIncludeSubfolders();
        Files.walkFileTree(rootPath, EnumSet.noneOf(FileVisitOption.class), includeSubfolders ? Integer.MAX_VALUE : 1,
            visitor);
        m_fileFilterStatistic = visitor.getFileFilterStatistic();
        final List<?> paths = visitor.getPaths();
        @SuppressWarnings("unchecked") // we know it better
        final List<FSPath> fsPaths = (List<FSPath>)paths;
        return fsPaths;
    }

    private FilterVisitor createVisitor() {
        final SettingsModelFilterMode settings = m_settings.getFilterModeModel();
        final boolean includeSubfolders = settings.isIncludeSubfolders();
        final FileAndFolderFilter filter = new FileAndFolderFilter(settings.getFilterOptionsSettings());
        final FilterMode filterMode = getFilterMode();
        switch (filterMode) {
            case FILES_AND_FOLDERS:
                return new FilterVisitor(filter, true, true, includeSubfolders);
            case FILES_IN_FOLDERS:
                return new FilterVisitor(filter, true, false, includeSubfolders);
            case FOLDERS:
                return new FilterVisitor(filter, false, true, includeSubfolders);
            case FOLDER:
            case FILE:
                throw new IllegalStateException(
                    "Encountered file/folder filter mode when walking the file tree. This is a coding error.");
            default:
                throw new IllegalStateException("Unknown filter mode: " + filterMode);
        }
    }

    private FilterMode getFilterMode() {
        return m_settings.getFilterModeModel().getFilterMode();
    }

    @Override
    public void close() throws IOException {
        if (m_fileSystem != null) {
            m_fileSystem.close();
            m_connection.close();
        }
    }

    @Override
    public FSPath getRootPath(final Consumer<StatusMessage> statusMessageConsumer)
        throws IOException, InvalidSettingsException {
        return getOutputPath(statusMessageConsumer);
    }

}