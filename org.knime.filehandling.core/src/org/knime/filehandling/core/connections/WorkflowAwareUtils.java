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
 *   May 6, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.connections;

import java.nio.file.AccessMode;
import java.nio.file.FileSystemException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Contains utility methods for {@link WorkflowAware} file systems.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public final class WorkflowAwareUtils {

    private static final String ACCESS_INSIDE_WORKFLOW_ERROR =
        "It is not possible to access paths inside of a workflow/component";

    private WorkflowAwareUtils() {
        // static utility class
    }

    /**
     * Creates a standardized exception for {@link WorkflowAware} file systems telling the user that reading from a path
     * pointing to a workflow is not supported.
     *
     * @param workflowPath path to a workflow or component
     * @return a {@link FileSystemException} that tells the user that reading workflows is not supported
     */
    public static FileSystemException createReadingKnimeObjectException(final String workflowPath) {
        return createKnimeObjectOperationException(workflowPath, "reading");
    }

    /**
     * Creates a standardized exception for {@link WorkflowAware} file sytems telling the user that writing to a path
     * pointing to a workflow is not supported.
     *
     * @param workflowPath path to a workflow or component
     * @return a {@link FileSystemException} that tells the user that writing a workflow is not supported
     */
    public static FileSystemException createWritingKnimeObjectException(final String workflowPath) {
        return createKnimeObjectOperationException(workflowPath, "writing to");
    }

    private static FileSystemException createKnimeObjectOperationException(final String workflowPath,
        final String operation) {
        final String reason = String
            .format("Only %s data items is supported (not workflows, components, workflow groups, etc)", operation);
        return new FileSystemException(workflowPath, null, reason);
    }

    /**
     * Creates a standardized exception for {@link WorkflowAware} file systems telling the user that accessing a
     * workflow in the provided manners is not supported.
     *
     * @param workflowPath path to a workflow or component
     * @param modes the {@link AccessMode AccessModes} must not be empty or null
     * @return a {@link FileSystemException} that tells the user that accessing a workflow in the provided manner is not
     *         supported
     */
    public static FileSystemException createAccessKnimeObjectException(final String workflowPath,
        final AccessMode[] modes) {
        final String accessModeString = Arrays.stream(modes)//
            .map(AccessMode::toString)//
            .map(String::toLowerCase)//
            .collect(Collectors.joining("/"));
        final String reason = String.format("The access mode%s %s are not supported on workflows or components.",
            modes.length > 1 ? 1 : 0, accessModeString);
        return new FileSystemException(workflowPath, null, reason);
    }

    /**
     * Creates a standardized exception for the case that users try to access paths within a workflow.
     *
     * @param pathIntoWorkflow the path into the workflow
     * @return a {@link FileSystemException} that tells the user that accessing paths within a workflow is not allowed
     */
    public static FileSystemException createAccessInsideWorkflowException(final String pathIntoWorkflow) {
        // TODO should this be a AccessDeniedException instead?
        return new FileSystemException(pathIntoWorkflow, null, ACCESS_INSIDE_WORKFLOW_ERROR);
    }
}
