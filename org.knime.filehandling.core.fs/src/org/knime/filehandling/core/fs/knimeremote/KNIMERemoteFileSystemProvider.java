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
 *   Nov 11, 2019 (Tobias Urhaug, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.filehandling.core.fs.knimeremote;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.core.runtime.CoreException;
import org.knime.core.util.FileUtil;
import org.knime.filehandling.core.connections.WorkflowAware;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;
import org.knime.filehandling.core.util.MountPointFileSystemAccessService;

/**
 * {@link FileSystemProvider} for the Explorer-based Mountpoint file system.
 *
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 */
class KNIMERemoteFileSystemProvider extends BaseFileSystemProvider<KNIMERemotePath, KNIMERemoteFileSystem>
    implements WorkflowAware {

    @Override
    protected SeekableByteChannel newByteChannelInternal(final KNIMERemotePath path, final Set<? extends OpenOption> options,
        final FileAttribute<?>... attrs) throws IOException {
        return new KNIMERemoteTempFileSeekableChannel(path, options);
    }

    private static int getTimeout() {
        return FileUtil.getDefaultURLTimeoutMillis();
    }

    @Override
    protected void createDirectoryInternal(final KNIMERemotePath dir, final FileAttribute<?>... attrs)
        throws IOException {
        MountPointFileSystemAccessService.instance().createDirectory(dir.toKNIMEProtocolURI());
    }

    @Override
    protected void copyInternal(final KNIMERemotePath source, final KNIMERemotePath target, final CopyOption... options) throws IOException {
        MountPointFileSystemAccessService.instance().copyFile(source.toKNIMEProtocolURI(), target.toKNIMEProtocolURI());
    }

    @Override
    protected void moveInternal(final KNIMERemotePath source, final KNIMERemotePath target, final CopyOption... options) throws IOException {
        MountPointFileSystemAccessService.instance().moveFile(source.toKNIMEProtocolURI(), target.toKNIMEProtocolURI());
    }

    @Override
    protected void checkAccessInternal(final KNIMERemotePath path, final AccessMode... modes) throws IOException {
        // there is nothing we can do here
    }


    @Override
    protected boolean exists(final KNIMERemotePath path) {
        try {
            MountPointFileSystemAccessService.instance().getFileAttributes(path.toKNIMEProtocolURI());
            return true;
        } catch (final IOException ex) {
            return false;
        }
    }

    @Override
    protected InputStream newInputStreamInternal(final KNIMERemotePath path, final OpenOption... options) throws IOException {

        try {
            return path.openURLConnection(getTimeout()).getInputStream();
        } catch (IOException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof FileNotFoundException) {
                throw new NoSuchFileException(path.toString());
            } else if (isNoSuchFileOnServerMountpoint(rootCause)) {
                throw new NoSuchFileException(path.toString());
            } else {
                throw e;
            }
        }
    }

    private static boolean isNoSuchFileOnServerMountpoint(final Throwable rootCause) {
        return rootCause instanceof CoreException && (
                rootCause.getMessage().endsWith("file does not exist.") // reported by RestServerExplorerFileStore
                || rootCause.getMessage().endsWith("file has already been deleted.") // reported by RestServerExplorerFileStore
                || rootCause.getMessage().endsWith(" It doesn't exist.") // reported by EjbServerExplorerFileStore
                );
    }

    @Override
    protected OutputStream newOutputStreamInternal(final KNIMERemotePath path, final OpenOption... options)
        throws IOException {
        final URL knimeURL = path.toKNIMEProtocolURI().toURL();
        return FileUtil.openOutputStream(knimeURL, "PUT");
    }

    @Override
    protected Iterator<KNIMERemotePath> createPathIterator(final KNIMERemotePath dir, final Filter<? super Path> filter)
            throws IOException {
        try {
            return new KNIMERemotePathIterator(dir, filter);
        } catch (IOException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof FileNotFoundException) {
                throw new NoSuchFileException(dir.toString());
            } else if (isNoSuchFileOnServerMountpoint(rootCause)) {
                throw new NoSuchFileException(dir.toString());
            } else {
                throw e;
            }
        }
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final KNIMERemotePath path, final Class<?> type) throws IOException {
        return MountPointFileSystemAccessService.instance().getFileAttributes(path.toKNIMEProtocolURI());
    }

    @Override
    protected void deleteInternal(final KNIMERemotePath path) throws IOException {
        MountPointFileSystemAccessService.instance().deleteFile(path.toKNIMEProtocolURI());
    }

    @Override
    public void deployWorkflow(final File source, final Path dest, final boolean overwrite, final boolean attemptOpen)
        throws IOException {
        final KNIMERemotePath checkedPath = checkCastAndAbsolutizePath(dest);
        MountPointFileSystemAccessService.instance().deployWorkflow(source, checkedPath.toKNIMEProtocolURI(), overwrite, attemptOpen);
    }

    @Override
    public File toLocalWorkflowDir(final Path path) throws IOException {
        final KNIMERemotePath checkedPath = checkCastAndAbsolutizePath(path);
        return MountPointFileSystemAccessService.instance().toLocalWorkflowDir(checkedPath.toKNIMEProtocolURI());
    }

    @SuppressWarnings("resource")
    @Override
    public Optional<String> getMountID() {
        return Optional.of(getFileSystemInternal().getMountpoint());
    }
}
