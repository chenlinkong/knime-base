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
 *   Feb 2, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.node.table.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypePath;

/**
 * This {@link ProductionPathProvider} restricts the number of production paths per external type to exactly one, i.e.
 * from the external type to its default KNIME type. The only exception is the most general type for which all paths are
 * included if there is no specialized path to the KNIME data type.<br>
 * When collecting the available production paths for a particular external type, the provider starts at the external
 * type and adds its production path as well as the production paths of all its super types.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> the type to identify external data types
 */
public final class SuperProductionPathProvider<T> implements ProductionPathProvider<T> {

    private final ProducerRegistry<T, ?> m_producerRegistry;

    private final Function<T, DataType> m_defaultDataTypeProvider;

    private final TreeTypeHierarchy<T, T> m_typeHierarchy;

    private final Predicate<DataType> m_hasSpecializedPath;

    /**
     * Constructor.
     *
     * @param readAdapterFactory of the reader
     * @param typeHierarchy the {@link TypeHierarchy} of the reader
     * @param hasSpecializedPath a {@link Predicate} that checks if there is a specialized path for a {@link DataType}
     */
    public SuperProductionPathProvider(final ReadAdapterFactory<T, ?> readAdapterFactory,
        final TreeTypeHierarchy<T, T> typeHierarchy, final Predicate<DataType> hasSpecializedPath) {
        m_producerRegistry = readAdapterFactory.getProducerRegistry();
        m_defaultDataTypeProvider = readAdapterFactory::getDefaultType;
        m_typeHierarchy = typeHierarchy;
        m_hasSpecializedPath = hasSpecializedPath;
    }

    @Override
    public ProductionPath getDefaultProductionPath(final T externalType) {
        final DataType defaultKnimeType = m_defaultDataTypeProvider.apply(externalType);
        return m_producerRegistry.getAvailableProductionPaths(externalType).stream()//
            .filter(p -> hasKnimeType(p, defaultKnimeType))//
            .findFirst()//
            .orElseThrow(IllegalStateException::new);
    }

    private static boolean hasKnimeType(final ProductionPath productionPath, final DataType type) {
        return getKnimeType(productionPath).equals(type);
    }

    private static DataType getKnimeType(final ProductionPath productionPath) {
        return productionPath.getConverterFactory().getDestinationType();
    }

    @Override
    public List<ProductionPath> getAvailableProductionPaths(final T externalType) {
        // includes externalType
        final TypePath<T> superTypes = m_typeHierarchy.getSupportingTypes(externalType);

        // TODO what about cases where there are multiple valid production paths for the same KNIME data type?
        // For example Date and time in Parquet: If the external type is DATE_TIME then we want to use
        // DATE_TIME -> ZonedDateTime -> ZonedDataTimeCell however if the external type is String, then this String could
        // be a valid zoned date time string and we would want to use String -> String -> ZonedDateTimeCell

        final List<ProductionPath> prodPaths = new ArrayList<>();

        for (T type : superTypes) {
            prodPaths.add(getDefaultProductionPath(type));
        }

        final T mostGeneralType = superTypes.getMostGeneralType();

        final List<ProductionPath> otherPathsForMostGeneral =
            m_producerRegistry.getAvailableProductionPaths(mostGeneralType).stream()//
                .filter(p -> !m_hasSpecializedPath.test(getKnimeType(p)))//
                .collect(Collectors.toList());

        prodPaths.addAll(otherPathsForMostGeneral);

        return prodPaths;
    }

}
