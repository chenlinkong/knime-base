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
 *   Mar 3, 2021 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.io.filehandling.csv.reader.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

import org.knime.base.node.io.filehandling.csv.reader.OSIndependentNewLineReader;
import org.knime.base.node.io.filehandling.streams.CompressionAwareCountingInputStream;
import org.knime.core.node.NodeLogger;
import org.knime.filehandling.core.node.table.reader.config.TableReadConfig;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessible;
import org.knime.filehandling.core.node.table.reader.randomaccess.RandomAccessibleUtils;
import org.knime.filehandling.core.node.table.reader.read.Read;
import org.knime.filehandling.core.util.BomEncodingUtils;

import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

/**
 * Implements {@link Read} specific to CSV table reader, based on univocity's {@link CsvParser}.
 *
 * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
 */
final class CsvRead implements Read<String> {

    /** */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(CsvRead.class);

    /** a parser used to parse the file */
    private final CsvParser m_parser;

    /** the reader reading from m_countingStream */
    private final BufferedReader m_reader;

    /** the size of the file being read */
    private final long m_size;

    /** the {@link CsvParserSettings} */
    private final CsvParserSettings m_csvParserSettings;

    /** The {@link CompressionAwareCountingInputStream} which creates the necessary streams */
    private final CompressionAwareCountingInputStream m_compressionAwareStream;

    /**
     * Constructor
     *
     * @param path the path of the file to read
     * @param config the CSV table reader configuration.
     * @throws IOException if a stream can not be created from the provided file.
     */
    @SuppressWarnings("resource") // The input stream is closed by the close method
    CsvRead(final Path path, final TableReadConfig<CSVTableReaderConfig> config) throws IOException {
        this(new CompressionAwareCountingInputStream(path), Files.size(path), config);//NOSONAR
    }

    /**
     * Constructor
     *
     * @param inputStream the {@link InputStream} to read from
     * @param config the CSV table reader configuration.
     * @throws IOException if a stream can not be created from the provided file.
     */
    @SuppressWarnings("resource") //streams will be closed in the close method
    CsvRead(final InputStream inputStream, final TableReadConfig<CSVTableReaderConfig> config) throws IOException {
        this(new CompressionAwareCountingInputStream(inputStream), -1, config);
    }

    private CsvRead(final CompressionAwareCountingInputStream inputStream, final long size,
        final TableReadConfig<CSVTableReaderConfig> config) throws IOException {
        m_size = size;
        m_compressionAwareStream = inputStream;

        final CSVTableReaderConfig csvReaderConfig = config.getReaderSpecificConfig();
        // Get the Univocity Parser settings from the reader specific configuration.
        m_csvParserSettings = csvReaderConfig.getCsvSettings();
        m_reader = createReader(csvReaderConfig);
        if (csvReaderConfig.skipLines()) {
            skipLines(csvReaderConfig.getNumLinesToSkip());
        }
        m_parser = new CsvParser(m_csvParserSettings);
        m_parser.beginParsing(m_reader);
    }

    @SuppressWarnings("resource")
    private BufferedReader createReader(final CSVTableReaderConfig csvReaderConfig) {
        final String charSetName = csvReaderConfig.getCharSetName();
        final Charset charset = charSetName == null ? Charset.defaultCharset() : Charset.forName(charSetName);
        if (csvReaderConfig.useLineBreakRowDelimiter()) {
            m_csvParserSettings.getFormat().setLineSeparator(OSIndependentNewLineReader.LINE_BREAK);
            return new BufferedReader(
                new OSIndependentNewLineReader(BomEncodingUtils.createReader(m_compressionAwareStream, charset)));
        } else {
            return BomEncodingUtils.createBufferedReader(m_compressionAwareStream, charset);
        }
    }

    @Override
    public RandomAccessible<String> next() throws IOException {
        String[] row = null;
        try {
            row = m_parser.parseNext();
        } catch (final TextParsingException e) {
            //Log original exception message
            LOGGER.debug(e.getMessage(), e);
            final Throwable cause = e.getCause();
            if (cause instanceof ArrayIndexOutOfBoundsException) {
                //Exception handling in case maxCharsPerCol or maxCols are exceeded like in the AbstractParser
                final int index = extractErrorIndex(cause);
                // for some reason when running in non-debug mode the memory limit per column exception often
                // contains a null message
                if (index == m_csvParserSettings.getMaxCharsPerColumn() || e.getCause().getMessage() == null) {
                    throw new IOException("Memory limit per column exceeded. Please adapt the according setting.");
                } else if (index == m_csvParserSettings.getMaxColumns()) {
                    throw new IOException("Number of parsed columns exceeds the defined limit ("
                        + m_csvParserSettings.getMaxColumns() + "). Please adapt the according setting.");
                } else {
                    // fall through to default exception
                }
            }
            throw new IOException(
                "Something went wrong during the parsing process. For further details please have a look into "
                    + "the log.",
                e);

        }
        return row == null ? null : RandomAccessibleUtils.createFromArrayUnsafe(row);
    }

    private static int extractErrorIndex(final Throwable cause) {
        try {
            return Integer.parseInt(cause.getMessage());
        } catch (NumberFormatException ex) {
            return -1;
        }
    }

    @Override
    public void close() throws IOException {
        m_parser.stopParsing();
        // the parser should already close the reader and the streams but we close them anyway just to be sure
        m_reader.close();
        m_compressionAwareStream.close();
    }

    @Override
    public OptionalLong getMaxProgress() {
        return m_size < 0 ? OptionalLong.empty() : OptionalLong.of(m_size);
    }

    @Override
    public long getProgress() {
        return m_compressionAwareStream.getCount();
    }

    /**
     * Skips n lines from m_countingStream. The method supports different newline schemes (\n \r \r\n)
     *
     * @param n the number of lines to skip
     * @throws IOException if reading from the stream fails
     */
    private void skipLines(final long n) throws IOException {
        for (int i = 0; i < n; i++) {
            m_reader.readLine(); //NOSONAR
        }
    }

}