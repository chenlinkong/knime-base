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
 *   Dec 22, 2020 (Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.filehandling.utility.nodes.pathtouri;

import java.util.Optional;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * The {@link NodeFactory} to create the node model allowing to convert paths to URLs.
 *
 * @author Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany
 */
public final class PathToUriNodeFactory extends ConfigurableNodeFactory<PathToUriNodeModel> {

    @Override
    public PathToUriNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfig = creationConfig.getPortConfig().orElseThrow(IllegalStateException::new);
        return new PathToUriNodeModel(portsConfig, createSettings(portsConfig));
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final PortsConfigurationBuilder builder = new PortsConfigurationBuilder();
        builder.addOptionalInputPortGroup(PathToUriNodeConfig.CONNECTION_INPUT_PORT_GRP_NAME,
            FileSystemPortObject.TYPE);
        builder.addFixedInputPortGroup(PathToUriNodeConfig.DATA_TABLE_INPUT_PORT_GRP_NAME, BufferedDataTable.TYPE);
        builder.addFixedOutputPortGroup(PathToUriNodeConfig.DATA_TABLE_OUTPUT_PORT_GRP_NAME, BufferedDataTable.TYPE);
        return Optional.of(builder);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfiguration = creationConfig.getPortConfig().orElseThrow(IllegalStateException::new);
        return new PathToUriNodeDialog(createSettings(portsConfiguration));
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<PathToUriNodeModel> createNodeView(final int viewIndex, final PathToUriNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    /**
     * Creates an PathToUrlNodeConfig object, which wraps the settings models for this node
     *
     * @param portsConfiguration The ports configuration
     *
     * @return An PathToUrlNodeConfig object
     */
    private static PathToUriNodeConfig createSettings(final PortsConfiguration portsConfiguration) {
        return new PathToUriNodeConfig(portsConfiguration);
    }
}
