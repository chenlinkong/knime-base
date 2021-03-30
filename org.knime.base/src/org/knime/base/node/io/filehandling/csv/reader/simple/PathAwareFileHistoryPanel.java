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
 *   May 27, 2020 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.io.filehandling.csv.reader.simple;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.FilesHistoryPanel;
import org.knime.core.node.util.FilesHistoryPanel.LocationValidation;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.ExceptionUtil;
import org.knime.filehandling.core.defaultnodesettings.FileSystemHelper;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.FileFilterStatistic;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.ReadPathAccessor;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
import org.knime.filehandling.core.node.table.reader.paths.PathSettings;

/**
 * Wrapper class for {@link FilesHistoryPanel} that implements {@link PathSettings}.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class PathAwareFileHistoryPanel implements PathSettings {

    private FilesHistoryPanel m_filePanel;

    private JSpinner m_timeoutSpinner;

    static final String CFG_KEY_LOCATION = "file_location";

    private static final String CFG_KEY_TIMEOUT = "connection_timeout";

    private static final int DEFAULT_URL_TIMEOUT_SECONDS = 1;

    private static final String DEFAULT_FILE = "";

    private String m_selectedFile = DEFAULT_FILE;

    JPanel createFilePanel() {
        final JPanel filePanel = new JPanel(new GridBagLayout());
        filePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Input location"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 3;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        final FilesHistoryPanel fileHistoryPanel = getFileHistoryPanel();
        filePanel.add(fileHistoryPanel, gbc);
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 5, 5);
        filePanel.add(new JLabel("Connection timeout [s]"), gbc);
        gbc.gridx++;
        m_timeoutSpinner = new JSpinner(new SpinnerNumberModel(DEFAULT_URL_TIMEOUT_SECONDS, 1, Integer.MAX_VALUE, 1));
        m_timeoutSpinner.setPreferredSize(new Dimension(70, m_timeoutSpinner.getPreferredSize().height));
        filePanel.add(m_timeoutSpinner, gbc);
        return filePanel;
    }

    void createFileHistoryPanel(final FlowVariableModel fvm, final String historyID) {
        if (m_filePanel == null) {
            m_filePanel = new FilesHistoryPanel(fvm, historyID, LocationValidation.FileInput, "");
            m_filePanel.setDialogType(JFileChooser.OPEN_DIALOG);
            m_filePanel.setToolTipText("Enter an URL of an ASCII datafile, select from recent files, or browse");
        }
    }

    FilesHistoryPanel getFileHistoryPanel() {
        return m_filePanel;
    }

    void setPath(final String url) {
        if (m_filePanel == null) {
            m_selectedFile = url;
        } else {
            m_filePanel.setSelectedFile(url);
        }
    }

    @Override
    public String getPath() {
        if (m_filePanel == null) {
            // accessing from node model
            return m_selectedFile;
        }
        return m_filePanel.getSelectedFile();
    }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_KEY_LOCATION, getPath().trim());
        if (m_timeoutSpinner != null) {
            settings.addInt(CFG_KEY_TIMEOUT, (int)m_timeoutSpinner.getValue());
        } else {
            settings.addInt(CFG_KEY_TIMEOUT, DEFAULT_URL_TIMEOUT_SECONDS);
        }
        if (m_filePanel != null) {
            m_filePanel.addToHistory();
        }
    }

    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_selectedFile = settings.getString(CFG_KEY_LOCATION, DEFAULT_FILE);
        if (m_timeoutSpinner != null) {
            m_timeoutSpinner.setValue(settings.getInt(CFG_KEY_TIMEOUT, DEFAULT_URL_TIMEOUT_SECONDS));
        }
        if (m_filePanel != null) {
            m_filePanel.setSelectedFile(m_selectedFile);
        }
    }

    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_KEY_LOCATION);
        settings.getInt(CFG_KEY_TIMEOUT);
    }

    @Override
    public void configureInModel(final PortObjectSpec[] specs, final Consumer<StatusMessage> statusMessageConsumer)
        throws InvalidSettingsException {
        if (!hasPath()) {
            throw new InvalidSettingsException("Please specify a location");
        }
    }

    @Override
    public ReadPathAccessor createReadPathAccessor() {
        final int timeoutInSec =
            m_timeoutSpinner != null ? (int)m_timeoutSpinner.getValue() : DEFAULT_URL_TIMEOUT_SECONDS;
        return new PathAwareReadAccessor(getPath(), timeoutInSec * 1000L);
    }

    private boolean hasPath() {
        final String p = getPath();
        return p != null && !p.trim().isEmpty();
    }

    private static class PathAwareReadAccessor implements ReadPathAccessor {

        private final String m_path;

        private final long m_timeout;

        private FSConnection m_connection;

        private FSLocation m_fsLocation;

        private boolean m_wasClosed = false;

        PathAwareReadAccessor(final String path, final long timeout) {
            m_path = path;
            m_timeout = timeout;
        }

        @Override
        public void close() throws IOException {
            if (m_connection != null) {
                m_connection.close();
            }
            m_wasClosed = true;
            m_connection = null;
        }

        @Override
        public List<FSPath> getFSPaths(final Consumer<StatusMessage> statusMessageConsumer)
            throws IOException, InvalidSettingsException {
            return Arrays.asList(getRootPath(statusMessageConsumer));
        }

        @SuppressWarnings("resource")
        @Override
        public FSPath getRootPath(final Consumer<StatusMessage> statusMessageConsumer)
            throws InvalidSettingsException, IOException {
            if (m_wasClosed) {
                throw new ClosedFileSystemException();
            }
            if (m_connection == null) {
                if (isURL()) {
                    m_fsLocation = new FSLocation(FSCategory.CUSTOM_URL, String.valueOf(m_timeout), m_path);
                } else {
                    m_fsLocation = new FSLocation(FSCategory.LOCAL, m_path);
                }
            }
            m_connection = FileSystemHelper.retrieveFSConnection(Optional.empty(), m_fsLocation)
                .orElseThrow(IllegalStateException::new);
            final FSPath rootPath = m_connection.getFileSystem().getPath(m_fsLocation);
            CheckUtils.checkSetting(!rootPath.toString().trim().isEmpty(), "Please specify a file.");
            CheckUtils.checkSetting(FSFiles.exists(rootPath), "The specified file %s does not exist.", rootPath);
            if (!Files.isReadable(rootPath)) {
                throw ExceptionUtil.createAccessDeniedException(rootPath);
            }
            final BasicFileAttributes attr = Files.readAttributes(rootPath, BasicFileAttributes.class);
            CheckUtils.checkSetting(attr.isRegularFile(), "%s is not a regular file. Please specify a file.", rootPath);
            return rootPath;
        }

        @SuppressWarnings("unused")
        private boolean isURL() {
            try {
                new URL(m_path.replace(" ", "%20"));
                return true;
            } catch (final MalformedURLException e) {
                return false;
            }
        }

        @Override
        public FileFilterStatistic getFileFilterStatistic() {
            CheckUtils.checkState(m_connection != null,
                "No statistic available. Call getFSPaths() or getPaths() first.");
            return new FileFilterStatistic(0, 0, 0, 1, 0, 0, 0);
        }

    }

}
