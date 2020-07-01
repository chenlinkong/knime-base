/*
 * ------------------------------------------------------------------------
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
 *   27.07.2007 (thor): created
 */
package org.knime.base.node.preproc.joiner3;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import org.knime.base.node.preproc.joiner3.Joiner3Settings.ColumnNameDisambiguation;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.CompositionMode;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.join.JoinImplementation;
import org.knime.core.data.join.JoinSpecification;
import org.knime.core.data.join.JoinSpecification.InputTable;
import org.knime.core.data.join.JoinTableSettings;
import org.knime.core.data.join.JoinerFactory.JoinAlgorithm;
import org.knime.core.data.join.results.JoinContainer;
import org.knime.core.data.join.results.JoinResults.OutputCombined;
import org.knime.core.data.join.results.JoinResults.OutputSplit;
import org.knime.core.data.join.results.JoinResults.ResultType;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.property.hilite.DefaultHiLiteMapper;
import org.knime.core.node.property.hilite.HiLiteHandler;
import org.knime.core.node.property.hilite.HiLiteTranslator;

/**
 * This is the model of the joiner node. It delegates the dirty work to the Joiner class.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 */
class Joiner3NodeModel extends NodeModel {

    private static final int MATCHES = 0, LEFT_OUTER = 1, RIGHT_OUTER = 2;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Joiner3NodeModel.class);

    private Joiner3Settings m_settings;

    private Hiliter m_hiliter = new Hiliter();

    Joiner3NodeModel(final Joiner3Settings settings) {
        super(2, 3);
        m_settings = settings;
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        JoinSpecification joinSpecification = joinSpecificationForSpecs(inSpecs);
        DataTableSpec matchSpec = joinSpecification.specForMatchTable();

        if (m_settings.getDuplicateHandling() == ColumnNameDisambiguation.DO_NOT_EXECUTE) {
            Optional<String> duplicateColumn = checkForDuplicateColumn(joinSpecification);
            if (duplicateColumn.isPresent()) {
                throw new InvalidSettingsException(String.format("Do not execute on ambiguous column names selected. "
                    + "Column %s appears both in left and right table.", duplicateColumn.get()));
            }
        }

        PortObjectSpec[] portObjectSpecs = new PortObjectSpec[3];
        if (m_settings.isOutputUnmatchedRowsToSeparateOutputPort()) {
            // split output
            portObjectSpecs[MATCHES] = matchSpec;
            portObjectSpecs[LEFT_OUTER] = joinSpecification.specForUnmatched(InputTable.LEFT);
            portObjectSpecs[RIGHT_OUTER] = joinSpecification.specForUnmatched(InputTable.RIGHT);
            return portObjectSpecs;
        } else {
            // single table output
            portObjectSpecs[MATCHES] = matchSpec;
            portObjectSpecs[LEFT_OUTER] = InactiveBranchPortObjectSpec.INSTANCE;
            portObjectSpecs[RIGHT_OUTER] = InactiveBranchPortObjectSpec.INSTANCE;
            return portObjectSpecs;
        }

    }

    @Override
    protected PortObject[] execute(final PortObject[] inPortObjects, final ExecutionContext exec) throws Exception {

        BufferedDataTable left = (BufferedDataTable) inPortObjects[0];
        BufferedDataTable right = (BufferedDataTable) inPortObjects[1];

        JoinSpecification joinSpecification = joinSpecificationForTables(left, right);

        JoinImplementation implementation = JoinAlgorithm.HYBRID_HASH.getFactory().create(joinSpecification, exec);
        implementation.setMaxOpenFiles(m_settings.getMaxOpenFiles());
        implementation.setEnableHiliting(m_settings.isHilitingEnabled());

        final PortObject[] outPortObjects = new PortObject[3];
        JoinContainer results;
        if (m_settings.isOutputUnmatchedRowsToSeparateOutputPort()) {
            // split output
            OutputSplit joinedTable = implementation.joinOutputSplit();
            results = (JoinContainer)joinedTable;
            outPortObjects[MATCHES] = joinedTable.getMatches();
            outPortObjects[LEFT_OUTER] = joinedTable.getLeftOuter();
            outPortObjects[RIGHT_OUTER] = joinedTable.getRightOuter();
        } else {
            // single table output
            OutputCombined joinedTable = implementation.joinOutputCombined();
            results = (JoinContainer)joinedTable;

            Arrays.fill(outPortObjects, InactiveBranchPortObject.INSTANCE);
            outPortObjects[MATCHES] = joinedTable.getTable();
        }
        m_hiliter.setResults(results);
        return outPortObjects;
    }

    /**
     * Create the join specification from the {@link #m_settings} for the given table specs.
     *
     * @throws InvalidSettingsException
     */
    private JoinSpecification joinSpecificationForSpecs(final PortObjectSpec... portSpecs)
        throws InvalidSettingsException {

        DataTableSpec left = (DataTableSpec)portSpecs[0];
        DataTableSpec right = (DataTableSpec)portSpecs[1];

        // left (top port) input table
        JoinTableSettings leftSettings = new JoinTableSettings(m_settings.getJoinMode().isIncludeLeftUnmatchedRows(),
            m_settings.getLeftJoinColumns(), m_settings.getLeftColumnSelectionConfig().applyTo(left).getIncludes(),
            InputTable.LEFT, left);

        // right (bottom port) input table
        JoinTableSettings rightSettings = new JoinTableSettings(m_settings.getJoinMode().isIncludeRightUnmatchedRows(),
            m_settings.getRightJoinColumns(), m_settings.getRightColumnSelectionConfig().applyTo(right).getIncludes(),
            InputTable.RIGHT, right);

        BiFunction<DataRow, DataRow, RowKey> rowKeysFactory;
        if (m_settings.isAssignNewRowKeys()) {
            rowKeysFactory = JoinSpecification.createSequenceRowKeysFactory();
        } else {
            rowKeysFactory = JoinSpecification.createConcatRowKeysFactory(m_settings.getRowKeySeparator());
        }

        UnaryOperator<String> columnNameDisambiguator;
        // replace with custom
        if (m_settings.getDuplicateHandling() == ColumnNameDisambiguation.APPEND_SUFFIX) {
            columnNameDisambiguator = s -> s.concat(m_settings.getDuplicateColumnSuffix());
        } else {
            columnNameDisambiguator = s -> s.concat(" (#1)");
        }

        return new JoinSpecification.Builder(leftSettings, rightSettings)
            .conjunctive(m_settings.getCompositionMode() == CompositionMode.MatchAll)
            .outputRowOrder(m_settings.getOutputRowOrder())
            .retainMatched(m_settings.getJoinMode().isIncludeMatchingRows())
            .mergeJoinColumns(m_settings.isMergeJoinColumns()).columnNameDisambiguator(columnNameDisambiguator)
            .rowKeyFactory(rowKeysFactory).build();
    }

    /**
     * Throw an {@link InvalidSettingsException} if column names are ambiguous. Used for the
     *
     * @param joinSpecification
     */
    private static Optional<String> checkForDuplicateColumn(final JoinSpecification joinSpecification) {
        DataTableSpec matchTableSpec = joinSpecification.specForMatchTable();
        final String suffix = joinSpecification.getColumnNameDisambiguator().apply("");
        return matchTableSpec.stream()
            // find columns that end with the disambiguator suffix
            .map(DataColumnSpec::getName).filter(s -> s.endsWith(suffix)).findAny()
            .map(s -> s.substring(0, s.length() - suffix.length()));
    }

    /**
     * Create join specification for tables by passing their specs to {@link #joinSpecification(PortObjectSpec[])} and
     * setting the data afterwards.
     *
     * @param portObjects {@link BufferedDataTable}s in disguise
     */
    private JoinSpecification joinSpecificationForTables(final BufferedDataTable left, final BufferedDataTable right)
        throws InvalidSettingsException {

        JoinSpecification joinSpecification = joinSpecificationForSpecs(left.getSpec(), right.getSpec());
        joinSpecification.getSettings(InputTable.LEFT).setTable(left);
        joinSpecification.getSettings(InputTable.RIGHT).setTable(right);

        return joinSpecification;
    }

    @Override
    protected void reset() {
        // called for instance after changing something and confirming in the node dialog
        // or if something goes wrong in the node execution
        m_hiliter.reset();
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_hiliter.saveInternals(nodeInternDir);
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        m_hiliter.loadInternals(nodeInternDir);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Joiner3Settings validationSettings = new Joiner3Settings();
        validationSettings.loadSettings(settings);
        // TODO validation without specs
        //        JoinImplementation.validateSettings(s.getJoinSpecification());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsInModel(settings);
    }

    @Override
    protected void setInHiLiteHandler(final int inIndex, final HiLiteHandler handler) {
        // when changing the input, the mapping is invalidated until the result is computed
        InputTable side = inIndex == 0 ? InputTable.LEFT : InputTable.RIGHT;
        m_hiliter.setInputHandler(side, handler);
    }

    @Override
    protected HiLiteHandler getOutHiLiteHandler(final int outIndex) {
        return m_hiliter.getOutputHandler(outIndex).orElse(null);
    }

    /**
     * Defines which output port is associated to which result type, i.e., matches to port 0, left unmatched rows to
     * port 1, etc.
     *
     * @param outIndex the port offset
     * @return joiner result type delivered at the given port
     */
    private Optional<ResultType> outportIndexToResultType(final int outIndex) {
        if (m_settings.isOutputUnmatchedRowsToSeparateOutputPort()) {
            switch (outIndex) {
                case 0:
                    return Optional.of(ResultType.MATCHES);
                case 1:
                    return Optional.of(ResultType.LEFT_OUTER);
                case 2:
                    return Optional.of(ResultType.RIGHT_OUTER);
                default:
                    return Optional.empty();
            }
        } else {
            return outIndex == 0 ? Optional.of(ResultType.ALL) : Optional.empty();
        }
    }

    /**
     * Contains all utility methods for enabling hiliting (mapping from row keys in input tables to row keys in result).
     * The output handlers and the translators that use them as input are long living. When connecting a new new input
     * port hilite handler, the translators target handlers are set to that handler with
     * {@link #setInputHandler(InputTable, HiLiteHandler)}.
     */
    private class Hiliter {

        /** Store the hilite mapping in this file. It contains a node config w*/
        private static final String INTERNALS_FILE_NAME = "hilite_mapping.xml.gz";

        /**
         * For each output port, a handler is instantiated. The map contains handlers for each result type, irrespective
         * of whether split output or single table output is selected. If the output ports are changed,
         * {@link #getOutputHandler(int)} will simply return different handlers for port 0 (see
         * {@link Joiner3NodeModel#outportIndexToResultType(int)}).
         */
        private final EnumMap<ResultType, HiLiteHandler> m_outputHandlers = new EnumMap<>(ResultType.class);

        /**
         * For each sensible combination of input port and output port, a translator is instantiated. The translator
         * associates row keys from the output table with the keys of the rows in the input that produced the output
         * row.
         */
        private final EnumMap<InputTable, EnumMap<ResultType, HiLiteTranslator>> m_translators =
            new EnumMap<>(InputTable.class);

        public Hiliter() {
            // create an output port handler for each result type.
            // when changing from split output to single table output, getOutHiLiteHandler returns
            // the handler for ALL instead of the handler for MATCHES at port 0.
            Arrays.stream(ResultType.values()).forEach(type -> m_outputHandlers.put(type, new HiLiteHandler()));

            // create a translator between related input and output ports
            // basically each combination except for left -> right outer, right -> left outer
            EnumMap<ResultType, HiLiteTranslator> leftTranslators = new EnumMap<>(ResultType.class);
            leftTranslators.put(ResultType.ALL, new HiLiteTranslator(m_outputHandlers.get(ResultType.ALL)));
            leftTranslators.put(ResultType.MATCHES, new HiLiteTranslator(m_outputHandlers.get(ResultType.MATCHES)));
            leftTranslators.put(ResultType.LEFT_OUTER, new HiLiteTranslator(m_outputHandlers.get(ResultType.LEFT_OUTER)));

            EnumMap<ResultType, HiLiteTranslator> rightTranslators = new EnumMap<>(ResultType.class);
            rightTranslators.put(ResultType.ALL, new HiLiteTranslator(m_outputHandlers.get(ResultType.ALL)));
            rightTranslators.put(ResultType.MATCHES, new HiLiteTranslator(m_outputHandlers.get(ResultType.MATCHES)));
            rightTranslators.put(ResultType.RIGHT_OUTER, new HiLiteTranslator(m_outputHandlers.get(ResultType.RIGHT_OUTER)));

            m_translators.put(InputTable.LEFT, leftTranslators);
            m_translators.put(InputTable.RIGHT, rightTranslators);
        }

        /**
         * Create an output handler for each output port. Create translators that connect each input port to each output
         * port.
         *
         * @param results provides {@link JoinContainer#getHiliteMapping(InputTable, ResultType)} to obtain mappings.
         */
        public void setResults(final JoinContainer results) {

            if (!m_settings.isHilitingEnabled()) {
                return;
            }

            // create hiliting functionality for each output port
            if (m_settings.isOutputUnmatchedRowsToSeparateOutputPort()) {
                // create a translator for each (input handler, output handler) pair
                setTranslatorMapping(results, InputTable.LEFT, ResultType.MATCHES);
                setTranslatorMapping(results, InputTable.LEFT, ResultType.LEFT_OUTER);

                setTranslatorMapping(results, InputTable.RIGHT, ResultType.MATCHES);
                setTranslatorMapping(results, InputTable.RIGHT, ResultType.RIGHT_OUTER);

            } else {
                // create one translator for each input table to the combined output
                setTranslatorMapping(results, InputTable.LEFT, ResultType.ALL);
                setTranslatorMapping(results, InputTable.RIGHT, ResultType.ALL);
            }
        }

        /**
         * Updates a translator using the mapping provided by a {@link JoinImplementation}. Used when setting state via
         * {@link #setResults(JoinContainer)}.
         *
         * @param results provides {@link JoinContainer#getHiliteMapping(InputTable, ResultType)} to obtain mappings.
         * @param side left or right input table
         * @param resultType the output port for matches, left unmatched rows, right unmatched rows, or combined results
         */
        private void setTranslatorMapping(final JoinContainer results, final InputTable side, final ResultType resultType) {
            // this translator connects the handler for the left/right input table...
            HiLiteTranslator translator = m_translators.get(side).get(resultType);
            // ... by mapping row keys from the input table to row keys from the output table
            Map<RowKey, Set<RowKey>> mapping =
                results.getHiliteMapping(side, resultType).orElseThrow(IllegalStateException::new);
            translator.setMapper(new DefaultHiLiteMapper(mapping));
        }

        /**
         * Invert {@link #saveInternals(File)}.
         * @param nodeInternDir passed through from  {@link Joiner3NodeModel#loadInternals(File, ExecutionMonitor)}
         */
        public void loadInternals(final File nodeInternDir) {

            if (m_settings == null || !m_settings.isHilitingEnabled()) {
                return;
            }

            try (FileInputStream in = new FileInputStream(new File(nodeInternDir, INTERNALS_FILE_NAME))) {
                NodeSettingsRO settings = NodeSettings.loadFromXML(in);

                if (m_settings.isOutputUnmatchedRowsToSeparateOutputPort()) {
                    setTranslatorMapping(settings, InputTable.LEFT, ResultType.MATCHES);
                    setTranslatorMapping(settings, InputTable.LEFT, ResultType.LEFT_OUTER);

                    setTranslatorMapping(settings, InputTable.RIGHT, ResultType.MATCHES);
                    setTranslatorMapping(settings, InputTable.RIGHT, ResultType.RIGHT_OUTER);
                } else {
                    setTranslatorMapping(settings, InputTable.LEFT, ResultType.ALL);
                    setTranslatorMapping(settings, InputTable.RIGHT, ResultType.ALL);
                }

            } catch (IOException | InvalidSettingsException e1) {
                LOGGER.error(e1);
            }
        }

        /**
         * Used when restoring state from {@link #loadInternals(File)}.
         *
         * @param mapper
         */
        private void setTranslatorMapping(final NodeSettingsRO settings, final InputTable inPort, final ResultType outPort)
            throws InvalidSettingsException {

            String settingsName = hiliteConfigurationName(inPort, outPort);
            DefaultHiLiteMapper mapper = DefaultHiLiteMapper.load(settings.getNodeSettings(settingsName));

            m_translators.computeIfAbsent(inPort, k -> new EnumMap<>(ResultType.class))
                .computeIfAbsent(outPort, k -> new HiLiteTranslator()).setMapper(mapper);
        }

        /**
         * Persist the mappings from output row keys to input row keys when saving the workflow.
         *
         * @param nodeInternDir passed through from {@link Joiner3NodeModel#saveInternals(File, ExecutionMonitor)}
         */
        public void saveInternals(final File nodeInternDir) {
            if (!m_settings.isHilitingEnabled()) {
                return;
            }

            final NodeSettings internalSettings = new NodeSettings("hilite_mapping");

            for (InputTable inPort : m_translators.keySet()) {
                for (ResultType outPort : m_translators.get(inPort).keySet()) {

                    String configurationName = hiliteConfigurationName(inPort, outPort);
                    NodeSettingsWO mappingSettings = internalSettings.addNodeSettings(configurationName);

                    DefaultHiLiteMapper mapper =
                        (DefaultHiLiteMapper)m_translators.get(inPort).get(outPort).getMapper();
                    if(mapper != null) {
                        mapper.save(mappingSettings);
                    }
                }
            }

            File f = new File(nodeInternDir, INTERNALS_FILE_NAME);
            try (FileOutputStream out = new FileOutputStream(f)) {
                internalSettings.saveToXML(out);
            } catch (IOException e) {
                LOGGER.warn(e);
            }

        }

        /**
         * When connecting a new input, the according hilite handler is set as new target for all involved translators.
         *
         * @param side whether a new handler for the left or right input table is provided
         * @param handler
         */
        public void setInputHandler(final InputTable side, final HiLiteHandler handler) {
            m_translators.get(side).values().forEach(translator -> {
                translator.removeAllToHiliteHandlers();
                translator.addToHiLiteHandler(handler);
            });
        }

        /**
         * Disable the mapping by discarding the hilite translators' mappers.
         */
        public void reset() {
            m_translators.values().stream().flatMap(map -> map.values().stream())
                .forEach(translator -> translator.setMapper(null));
        }

        /**
         * @param outIndex the port. returns the output handler for result type ALL for port 0 if single table output is
         *            on, otherwise the output handler for result type MATCHES.
         * @return the output handler for the given port. Empty optional if handler for a deactivated port is requested.
         */
        public Optional<HiLiteHandler> getOutputHandler(final int outIndex) {
            return outportIndexToResultType(outIndex).map(m_outputHandlers::get);
        }

        /**
         * Each translator's mapping connects one output port to an input port. When saving, each mapping gets its own
         * configuration key
         *
         * @return the configuration key for the given input port output port combination
         */
        private String hiliteConfigurationName(final InputTable inputPort, final ResultType outputPort) {
            return String.format("hiliteMapping_%s_to_%s", inputPort, outputPort);
        }

    }

}
