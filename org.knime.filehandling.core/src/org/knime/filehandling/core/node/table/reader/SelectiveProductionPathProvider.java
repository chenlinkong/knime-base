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
 *   Feb 4, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.node.table.reader;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TraversableTypeHierarchy;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TypePath;

/**
<<<<<<< HEAD
 * Uses a {@link BiPredicate} of the actual external type and the {@link ProductionPath} to decide if the
 * {@link ProductionPath} should be included or not. Candidates are all {@link ProductionPath ProductionPaths}
 * registered either for the external type itself or for any of its super-types.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @param <T> the type used to identify external data types
=======
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
>>>>>>> 2c7b98d71 (Custom ProductionPathProvider changes)
 */
public final class SelectiveProductionPathProvider<T> implements ProductionPathProvider<T> {

    private final ProducerRegistry<T, ?> m_producerRegistry;

    private final Function<T, DataType> m_defaultTypeProvider;

    private final TraversableTypeHierarchy<T, T> m_typeHierarchy;

    private final BiPredicate<T, ProductionPath> m_productionPathFilter;

    /**
     * @param producerRegistry provides production paths
     * @param defaultTypeProvider provides the default {@link DataType} for an external type
     * @param typeHierarchy of the external types, used to find the super types of an external type
     * @param productionPathFilter decides if a {@link ProductionPath} is valid for a given external type
     */
    public SelectiveProductionPathProvider(final ProducerRegistry<T, ?> producerRegistry,
        final Function<T, DataType> defaultTypeProvider, final TraversableTypeHierarchy<T, T> typeHierarchy,
        final BiPredicate<T, ProductionPath> productionPathFilter) {
        m_producerRegistry = producerRegistry;
        m_defaultTypeProvider = defaultTypeProvider;
        m_typeHierarchy = typeHierarchy;
        m_productionPathFilter = productionPathFilter;
    }

    @Override
    public ProductionPath getDefaultProductionPath(final T externalType) {
        final DataType knimeType = m_defaultTypeProvider.apply(externalType);
        return m_producerRegistry.getAvailableProductionPaths(externalType).stream()//
            .filter(p -> knimeType.equals(p.getDestinationType()))//
            .findFirst()//
            .orElseThrow(() -> new IllegalStateException(
                String.format("No production path available from %s to %s", externalType, knimeType)));
    }

    @Override
    public List<ProductionPath> getAvailableProductionPaths(final T externalType) {
        final TypePath<T> typePath = m_typeHierarchy.getSupportingTypes(externalType);
        return typePath.stream()//
            .map(m_producerRegistry::getAvailableProductionPaths)//
            .flatMap(List::stream)//
            .filter(p -> m_productionPathFilter.test(externalType, p))//
            .collect(toList());
    }

}
