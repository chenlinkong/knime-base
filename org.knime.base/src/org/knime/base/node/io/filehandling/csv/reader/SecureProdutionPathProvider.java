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
 *   Apr 29, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.io.filehandling.csv.reader;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.knime.core.data.DataType;
import org.knime.core.data.convert.datacell.JavaToDataCellConverterFactory;
import org.knime.core.data.convert.datacell.SecureJavaToDataCellConverterRegistry;
import org.knime.core.data.convert.map.CellValueProducerFactory;
import org.knime.core.data.convert.map.ProducerRegistry;
import org.knime.core.data.convert.map.ProductionPath;
import org.knime.core.data.convert.map.Source;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.node.table.reader.ProductionPathProvider;
import org.knime.filehandling.core.node.table.reader.type.hierarchy.TreeTypeHierarchy;

/**
 * A {@link ProductionPathProvider} that only provides a fixed set of {@link ProductionPath ProductionPaths}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class SecureProdutionPathProvider<T> implements ProductionPathProvider<T> {

    private final ProducerRegistry<T, ?> m_producerRegistry;

    private final TreeTypeHierarchy<T, T> m_typeHierarchy;

    private final Function<T, DataType> m_defaultTypeProvider;

    SecureProdutionPathProvider(final ProducerRegistry<T, ?> producerRegistry,
        final TreeTypeHierarchy<T, T> typeHiearchy, final Function<T, DataType> defaultTypeProvider) {
        m_producerRegistry = producerRegistry;
        m_typeHierarchy = typeHiearchy;
        m_defaultTypeProvider = defaultTypeProvider;
    }

    @Override
    public ProductionPath getDefaultProductionPath(final T externalType) {
        final DataType knimeType = m_defaultTypeProvider.apply(externalType);
        final CellValueProducerFactory<?, T, ?, ?> producerFactory =
            getProducerFactory(externalType, m_producerRegistry);
        final JavaToDataCellConverterFactory<?> converterFactory =
            getConverterFactory(producerFactory.getDestinationType(), knimeType);
        return new ProductionPath(producerFactory, converterFactory);
    }

    private static <J> JavaToDataCellConverterFactory<J> getConverterFactory(final Class<J> javaClass,
        final DataType destinationType) {
        return getConverterFactories(javaClass)//
            .filter(f -> destinationType.equals(f.getDestinationType()))//
            .findFirst()//
            .orElseThrow(() -> new IllegalStateException(
                "There exists not JavaToDataCellConverter for the intermediate type: " + javaClass));
    }

    private static <J> Stream<JavaToDataCellConverterFactory<J>> getConverterFactories(final Class<J> javaClass) {
        final List<JavaToDataCellConverterFactory<J>> factories =
            SecureJavaToDataCellConverterRegistry.INSTANCE.getConverterFactoriesBySourceType(javaClass);
        return factories.stream();
    }

    private <S extends Source<T>> CellValueProducerFactory<S, T, ?, ?> getProducerFactory(final T externalType,
        final ProducerRegistry<T, S> registry) {
        Collection<CellValueProducerFactory<S, T, ?, ?>> producerFactories =
            registry.getFactoriesForSourceType(externalType);
        CheckUtils.checkState(!producerFactories.isEmpty(),
            "Coding error: No producer factory registered for external type '%s'.", externalType);
        CheckUtils.checkState(producerFactories.size() == 1,
            "Coding error: More than one producer factory registered for external type '%s'.", externalType);
        return producerFactories.iterator().next();
    }

    @Override
    public List<ProductionPath> getAvailableProductionPaths(final T externalType) {
        final Map<DataType, ProductionPath> coveredTypes = new HashMap<>();
        m_typeHierarchy.walkUpToRoot(externalType, t -> {
            final ProductionPath defaultPath = getDefaultProductionPath(t.getType());
            coveredTypes.put(defaultPath.getDestinationType(), defaultPath);
        });
        addUncoveredRootPaths(coveredTypes);
        final Comparator<ProductionPath> comparator =
            Comparator.<ProductionPath, String> comparing(p -> p.getDestinationType().getName());
        return coveredTypes.values().stream()//
            .sorted(comparator).collect(Collectors.toList());
    }

    private void addUncoveredRootPaths(final Map<DataType, ProductionPath> coveredTypes) {
        final T rootType = m_typeHierarchy.getRootNode().getType();
        final List<ProductionPath> rootProdPaths = getPathsFor(rootType);
        for (ProductionPath path : rootProdPaths) {
            final DataType knimeType = path.getDestinationType();
            if (!coveredTypes.containsKey(knimeType)) {
                coveredTypes.put(knimeType, path);
            }
        }
    }

    private List<ProductionPath> getPathsFor(final T externalType) {
        final CellValueProducerFactory<?, T, ?, ?> producerFactory =
            getProducerFactory(externalType, m_producerRegistry);
        return getConverterFactories(producerFactory.getDestinationType())//
            .map(f -> new ProductionPath(producerFactory, f))//
            .collect(toList());
    }

}
