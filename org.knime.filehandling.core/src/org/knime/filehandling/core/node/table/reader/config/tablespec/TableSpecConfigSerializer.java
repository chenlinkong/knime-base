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
 *   Feb 3, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.node.table.reader.config.tablespec;

import java.util.EnumMap;

import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.filehandling.core.node.table.reader.selector.ColumnFilterMode;

/**
 * Serializer for {@link TableSpecConfig} objects.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> type used to identify external data types
 * @noreference non-public API
 * @noimplement non-public API
 */
public interface TableSpecConfigSerializer<T> extends NodeSettingsSerializer<TableSpecConfig<T>> {

    /**
     * Creates a {@link TableSpecConfigSerializerBuilder} for building a {@link TableSpecConfigSerializer}.
     *
     * @param <T> the type used to identify external data types
     * @param productionPathLoader loader for {@link ProductionPath ProductionPaths}
     * @param configIDLoader loader for the {@link ConfigID}
     * @param typeSerializer {@link NodeSettingsSerializer} for saving and loading the external data type
     * @return the builder
     */
    static <T> TableSpecConfigSerializerBuilder<T> builder(final ProductionPathSerializer productionPathLoader,
        final ConfigIDLoader configIDLoader, final NodeSettingsSerializer<T> typeSerializer) {
        return new TableSpecConfigSerializerBuilder<>(productionPathLoader, configIDLoader, typeSerializer);
    }

    /**
     * Creates a {@link TableSpecConfigSerializerBuilder} for building a {@link TableSpecConfigSerializer}.
     *
     * @param <T> the type used to identify external data types
     * @param producerRegistry to use for loading the {@link ProductionPath ProductionPaths}
     * @param configIDLoader loader for the {@link ConfigID}
     * @param typeSerializer {@link NodeSettingsSerializer} for saving and loading the external data type
     * @return the builder
     */
    static <T> TableSpecConfigSerializerBuilder<T> builder(final ProducerRegistry<T, ?> producerRegistry,
        final ConfigIDLoader configIDLoader, final NodeSettingsSerializer<T> typeSerializer) {
        final ProductionPathSerializer productionPathLoader = new DefaultProductionPathLoader(producerRegistry);
        return new TableSpecConfigSerializerBuilder<>(productionPathLoader, configIDLoader, typeSerializer);
    }

    /**
     * Setter for the skip empty columns option. This option is stored outside of the {@link TableSpecConfig}. Only
     * needed if the skip empty option is available in the reader. Defaults to {@code false}.
     *
     * @param skipEmptyColumns {@code true} if empty columns should be skipped
     */
    void setSkipEmptyColumns(final boolean skipEmptyColumns);

    /**
     * Setter for the {@link ColumnFilterMode} when stored outside of the {@link TableSpecConfig}. This setter is only
     * intended for the use in readers created with KNIME AP 4.2 i.e. the CSV and Simple File Reader. Newer reader
     * implementations should NOT store the {@link ColumnFilterMode} outside of the {@link TableSpecConfig}. Also note
     * that the set {@link ColumnFilterMode} is ignored if a {@link ColumnFilterMode} is found in the serialized
     * {@link TableSpecConfig}.
     *
     * @param columnFilterMode {@link ColumnFilterMode} to use
     */
    void setColumnFilterMode(final ColumnFilterMode columnFilterMode);

    /**
     * Enum of versions where the TableSpecConfig serialization changed.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    enum TableSpecConfigSerializerVersion {
            /**
             * Version 4.4.0. This is the version where this enum was first introduced. This version also introduced the
             * skipEmptyColumns option and separated storing of the ProductionPaths from storing the individual specs.
             */
            V4_4,
            /**
             * Version 4.3.0. This version introduced TableTransformations
             */
            V4_3,
            /**
             * Version 4.2.0. Introduced TableSpecConfig.
             */
            V4_2;
    }

    /**
     * Builder for {@link TableSpecConfigSerializer TableSpecConfigSerializers}. Allows clients to add support for
     * multiple versions depending on when they were first introduced.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     * @param <T> the type used to identify external data types
     */
    static final class TableSpecConfigSerializerBuilder<T> {

        private static final TableSpecConfigSerializerVersion CURRENT_VERSION = TableSpecConfigSerializerVersion.V4_4;

        private final ProductionPathSerializer m_productionPathLoader;

        private EnumMap<TableSpecConfigSerializerVersion, TableSpecConfigSerializer<T>> m_serializers =
            new EnumMap<>(TableSpecConfigSerializerVersion.class);

        private TableSpecConfigSerializerBuilder(final ProductionPathSerializer productionPathLoader,
            final ConfigIDLoader configIDLoader, final NodeSettingsSerializer<T> typeSerializer) {
            m_productionPathLoader = productionPathLoader;
            m_serializers.put(TableSpecConfigSerializerVersion.V4_4,
                new V44TableSpecConfigSerializer<>(productionPathLoader, configIDLoader, typeSerializer));
        }

        // TODO specify all constructor parameters? This would allow to switch out the different serializers between versions

        public TableSpecConfigSerializerBuilder<T> addV43Support() {
            m_serializers.put(TableSpecConfigSerializerVersion.V4_3,
                new V43TableSpecConfigSerializer<>(m_productionPathLoader));
            return this;
        }

        public TableSpecConfigSerializerBuilder<T> addV42Support(final ProducerRegistry<T, ?> producerRegistry,
            final T mostGenericType) {
            m_serializers.put(TableSpecConfigSerializerVersion.V4_2,
                new V42TableSpecConfigSerializer<>(producerRegistry, mostGenericType));
            return this;
        }

        public TableSpecConfigSerializer<T> build() {
            return new VersionedTableSpecConfigSerializer<>(m_serializers, CURRENT_VERSION);
        }

    }
}