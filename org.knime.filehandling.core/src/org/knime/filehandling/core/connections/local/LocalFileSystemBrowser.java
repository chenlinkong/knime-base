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
 */
package org.knime.filehandling.core.connections.local;

import java.io.File;
import java.nio.file.InvalidPathException;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileSystemView;
import javax.swing.filechooser.FileView;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.NodeContext;
import org.knime.filehandling.core.filechooser.AbstractFileChooserBrowser;

/**
 *
 * A file system browser that makes use of the {@link JFileChooser} to browse the local file system.
 *
 * @author Martin Horn, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
class LocalFileSystemBrowser extends AbstractFileChooserBrowser {

    /**
     *
     */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(LocalFileSystemBrowser.class);

    private final FileSystemView m_fileSystemView;

    private final LocalFileSystem m_fileSystem;

    /**
     * Constructor that will use the system default {@link FileSystemView}.
     *
     * @param fileSystem the {@link LocalFileSystem} to resolve paths with
     */
    public LocalFileSystemBrowser(final LocalFileSystem fileSystem) {
        this(fileSystem, null);
    }

    /**
     * Constructor that allows to pass in a custom {@link FileSystemView}.
     *
     * @param fileSystem the {@link LocalFileSystem} to resolve paths with
     * @param fileSystemView to use in the {@link JFileChooser}
     */
    public LocalFileSystemBrowser(final LocalFileSystem fileSystem, final FileSystemView fileSystemView) {
        m_fileSystemView = fileSystemView;
        m_fileSystem = fileSystem;
    }

    @Override
    public boolean isCompatible() {
        return NodeContext.getContext().getNodeContainer() != null;
    }

    @Override
    protected FileSystemView getFileSystemView() {
        return m_fileSystemView;
    }

    @Override
    protected FileView getFileView() {
        return null;
    }

    @Override
    protected File createFileFromPath(final String path) {
        try {
            return m_fileSystem.getPath(path).toAbsolutePath().toFile();
        } catch (InvalidPathException ex) {
            LOGGER.debug(String.format("Creating a path from the string '%s' failed.", path), ex);
            return null;
        }
    }

}
