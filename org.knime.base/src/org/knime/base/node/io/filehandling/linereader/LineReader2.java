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
 *   4 Nov 2020 (lars.schweikardt): created
 */
package org.knime.base.node.io.filehandling.linereader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.knime.core.columnar.batch.SequentialBatchReadable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.filehandling.core.node.table.reader.TableReader;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.spec.TypedReaderTableSpec;
import org.knime.filehandling.core.util.BomEncodingUtils;
import org.knime.filehandling.core.util.FileCompressionUtils;

/**
 * Reader for the line reader node.
 *
 * @author Lars Schweikardt, KNIME GmbH, Konstanz, Germany
 */
final class LineReader2 implements TableReader<LineReaderConfig2, Class<?>, String> {

    @Override
    public LineRead read(final Path path, final TableReadConfig<LineReaderConfig2> config) throws IOException {
        return new LineRead(path, config);
    }

    @Override
    public TypedReaderTableSpec<Class<?>> readSpec(final Path path, final TableReadConfig<LineReaderConfig2> config,
        final ExecutionMonitor exec) throws IOException {

        final String colName = config.useColumnHeaderIdx() ? getFirstLine(path, config)
            : config.getReaderSpecificConfig().getColumnHeaderName();
        return TypedReaderTableSpec.create(Collections.singleton(colName), Collections.singleton(String.class),
            Collections.singleton(Boolean.TRUE));
    }

    /**
     * Returns the first non empty line of the file to determine the column header.
     *
     * @param path the source file {@link Path}
     * @param config the {@link TableReadConfig}
     *
     * @return the content of the first line
     * @throws IOException
     */
    private static String getFirstLine(final Path path, final TableReadConfig<LineReaderConfig2> config)
        throws IOException {
        final String firstLine;
        final String charsetName = config.getReaderSpecificConfig().getCharSetName();
        final Charset charset = charsetName == null ? Charset.defaultCharset() : Charset.forName(charsetName);
        try (final InputStream in = FileCompressionUtils.createInputStream(path);
                final BufferedReader reader = BomEncodingUtils.createBufferedReader(in, charset);
                final Stream<String> currentLines = reader.lines()) {
            final Optional<String> optColName =
                currentLines.filter(s -> (!config.skipEmptyRows() || !s.trim().isEmpty())).findFirst();

            if (!optColName.isPresent() || optColName.get().trim().isEmpty()) {
                // if top line or all lines are blank in file use a default non-empty string
                firstLine = "<empty>";
            } else {
                firstLine = optColName.get();
            }
        }
        return firstLine;
    }

    @Override
    public SequentialBatchReadable readContent(final Path item, final TableReadConfig<LineReaderConfig2> config,
        final TypedReaderTableSpec<Class<?>> spec) {
        return new LineSequentialBatchReadable(item, config, 1024);
    }

}
