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
 *   Feb 11, 2020 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.filehandling.core.fs.knimerelativeto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.MetaNodeTemplateInformation;
import org.knime.core.node.workflow.WorkflowPersistor;
import org.knime.core.util.workflowalizer.MetadataConfig;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.RelativeTo;
import org.knime.filehandling.core.connections.WorkflowAwareErrorHandling.Entity;

/**
 * Local KNIME relative to File System implementation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class LocalRelativeToFileSystem extends BaseRelativeToFileSystem {

    /**
     * Where this file system is rooted in the local (platform default) file system.
     */
    private final Path m_localRoot;

    /**
     * Default constructor.
     *
     * @param uri URI without a path
     * @param localRoot Where this file system is rooted in the local (platform default) file system.
     * @param type The relative-to type of this file system (workflow, mountpoint, ...).
     * @param workingDir Path (in this file system) that specifies the working directory.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    LocalRelativeToFileSystem(final Path localRoot, //
        final RelativeTo type, //
        final String workingDir, //
        final FSLocationSpec fsLocationSpec) {

        super(new LocalRelativeToFileSystemProvider(), //
            type, //
            workingDir, //
            fsLocationSpec);

        CheckUtils.checkArgument(localRoot.getFileSystem() == FileSystems.getDefault(),
            "The local root of a LocalRelativeToFileSystem must be a path in the platform default file system");
        m_localRoot = localRoot.toAbsolutePath().normalize();
    }

    @Override
    public boolean isWorkflowDirectory(final RelativeToPath path) throws IOException {
        return isLocalWorkflowDirectory(toLocalPath(path));
    }

    @Override
    public Path toRealPathWithAccessibilityCheck(final RelativeToPath path) throws IOException {
        if (!isPathAccessible(path)) {
            throw new NoSuchFileException(path.toString());
        } else {
            return toLocalPath(path);
        }
    }

    @Override
    protected Optional<Entity> getEntity(final RelativeToPath path) throws IOException {
        // throws NoSuchFileException if it points into a workflow
        final Path localPath = toRealPathWithAccessibilityCheck(path);
        if (!Files.exists(localPath)) {
            return Optional.empty();
        } else if (!Files.isDirectory(localPath)) {
            return Optional.of(Entity.DATA);
        } else {
            // directories can be either workflows, meta nodes, components or workflow groups
            if (hasWorkflowFile(localPath)) {
                if (hasTemplateFile(localPath)) {
                    final Entity entity = getTemplateEntity(localPath);
                    return Optional.of(entity);
                } else {
                    return Optional.of(Entity.WORKFLOW);
                }
            } else {
                return Optional.of(Entity.WORKFLOW_GROUP);
            }
        }
    }

    private static Entity getTemplateEntity(final Path localPath) throws IOException {
        if (isComponent(localPath.resolve(WorkflowPersistor.TEMPLATE_FILE))) {
            return Entity.COMPONENT;
        } else {
            return Entity.METANODE;
        }
    }

    private static boolean hasTemplateFile(final Path localPath) {
        return Files.exists(localPath.resolve(WorkflowPersistor.TEMPLATE_FILE));
    }

    private static boolean hasWorkflowFile(final Path localPath) {
        return Files.exists(localPath.resolve(WorkflowPersistor.WORKFLOW_FILE));
    }

    private static boolean isComponent(final Path pathToTemplateFile) throws IOException {
        assert pathToTemplateFile.endsWith(WorkflowPersistor.TEMPLATE_FILE);
        final MetadataConfig c = new MetadataConfig("ignored");
        try (final InputStream s = Files.newInputStream(pathToTemplateFile)) {
            c.load(s);
            return c.getConfigBase("workflow_template_information").getString("templateType")
                    .equals(MetaNodeTemplateInformation.TemplateType.SubNode.toString());
        } catch (InvalidSettingsException ex) {
            throw new IOException("Invalid template.knime file.", ex);
        }
    }

    /**
     * Maps a path from relative-to file system to a path in the local file system.
     *
     * @param path a relative-to path inside relative-to file system
     * @return an absolute path in the local file system (default FS provider) that corresponds to this path.
     */
    public Path toLocalPath(final RelativeToPath path) {
        final RelativeToPath absolutePath = (RelativeToPath)path.toAbsolutePath().normalize();
        return Paths.get(m_localRoot.toString(), absolutePath.stringStream().toArray(String[]::new));
    }

    /**
     * @return where this file system is rooted in the local (platform default) file system.
     */
    public Path getLocalRoot() {
        return m_localRoot;
    }

    /**
     * @return where this file system's working directory is at in the local (platform default) file system.
     */
    public Path getLocalWorkingDir() {
        return toLocalPath(getWorkingDirectory()).toAbsolutePath().normalize();
    }

    @Override
    protected boolean existsWithAccessibilityCheck(final RelativeToPath path) throws IOException {
        final Path localPath = toLocalPath(path);
        return isPathAccessible(path) && Files.exists(localPath);
    }

    @Override
    protected void prepareClose() {
        // Nothing to do
    }
}
