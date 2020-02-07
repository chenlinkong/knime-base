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
 * -------------------------------------------------------------------
 * History
 *   23.01.2019 (Temesgen H. Dadi): created
 */
package org.knime.base.node.preproc.columnappend2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.OutputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.util.ButtonGroupEnumInterface;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.UniqueNameGenerator;

/**
 * This is the model implementation of ColumnAppender.
 *
 * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
 */

final class ColumnAppender2NodeModel extends NodeModel {

    /** Key for storing the selected way of getting rowIDs. */
    private static final String KEY_SELECTED_ROWID_MODE = "selected_rowid_mode";

    /** Key for storing the table (port index) used for rowIDs. */
    private static final String KEY_SELECTED_ROWID_TABLE = "selected_rowid_table";

    /** Different options of getting rowIDs. */
    private final SettingsModelString m_rowIDModeSettings = createRowIDModeSelectModel();

    /** Table number that decides the rowIDs. */
    private final SettingsModelIntegerBounded m_rowIDTableSettings = createRowIDTableSelectModel();

    private final int m_numInPorts;

    static SettingsModelString createRowIDModeSelectModel() {
        return new SettingsModelString(KEY_SELECTED_ROWID_MODE, RowKeyMode.IDENTICAL.getActionCommand());
    }

    static SettingsModelIntegerBounded createRowIDTableSelectModel() {
        return new SettingsModelIntegerBounded(KEY_SELECTED_ROWID_TABLE, 1, 1, Integer.MAX_VALUE);
    }

    /**
     * Constructor for dynamic ports.
     *
     * @param portsConfiguration Tthe ports configuration.
     */
    ColumnAppender2NodeModel(final PortsConfiguration portsConfiguration) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_numInPorts = portsConfiguration.getInputPorts().length;
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        // Sanity check settings even though dialog checks, in case any flow variables went bad.
        if (isSelectedMode(RowKeyMode.KEY_TABLE)) {
            CheckUtils.checkSetting(m_rowIDTableSettings.getIntValue() <= m_numInPorts,
                "The selected port number for row key must be a number between 1 and "
                    + "%s (the number of input tables)",
                m_numInPorts);
        }
        final DataTableSpec spec = combineToSingleSpec(createUniqueSpecs(inSpecs));
        return new DataTableSpec[]{spec};
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_rowIDModeSettings.saveSettingsTo(settings);
        m_rowIDTableSettings.saveSettingsTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_rowIDModeSettings.loadSettingsFrom(settings);
        m_rowIDTableSettings.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_rowIDModeSettings.validateSettings(settings);
        m_rowIDModeSettings.validateSettings(settings);
    }

    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        /* Nothing to do here. */
    }

    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        /* Nothing to do here. */
    }

    @Override
    protected void reset() {
        /* Nothing to do here. */
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        final BufferedDataTable out;
        if (isSelectedMode(RowKeyMode.IDENTICAL)) {
            out = joinTablesWithIdenticalKeys(inData, exec);
        } else {
            out = createNewTable(inData, exec);
        }
        return new BufferedDataTable[]{out};
    }

    /**
     * Given an array of BufferedDataTable, it returns their corresponding specs.
     *
     * @param inData an array of BufferedDataTable.
     * @return an array of DataTableSpec.
     */
    private static DataTableSpec[] getSpecFromInput(final BufferedDataTable[] inData) {
        return Arrays.stream(inData)//
            .map(BufferedDataTable::getDataTableSpec)//
            .toArray(DataTableSpec[]::new);
    }

    /**
     * Given an array of DataTableSpec, it returns same sized array of DataTableSpec in which all column names are
     * unique across all of the array elements. Redundant column names are resolved by appending "(#1)", "(#2)", and so
     * on as required.
     *
     * @param inSpecs an array of DataTableSpec.
     * @return a concatenation of the input Specs.
     */
    private static DataTableSpec[] createUniqueSpecs(final DataTableSpec[] inSpecs) {

        final UniqueNameGenerator nameGen = new UniqueNameGenerator((DataTableSpec)null);

        final DataTableSpec[] uniqDataTableSpecs = new DataTableSpec[inSpecs.length];
        for (int i = 0; i < inSpecs.length; i++) {
            final DataTableSpec curSpec = inSpecs[i];
            final DataColumnSpec[] outColSpecs = new DataColumnSpec[curSpec.getNumColumns()];
            for (int j = 0; j < curSpec.getNumColumns(); j++) {
                outColSpecs[j] = nameGen.newCreator(curSpec.getColumnSpec(j)).createSpec();
            }
            uniqDataTableSpecs[i] = new DataTableSpec(outColSpecs);
        }
        return uniqDataTableSpecs;
    }

    /**
     * Creates a single combined DataTableSpec from an array while respecting the order of the input DataTableSpecs.
     *
     * @param inSpecs an array of DataTableSpec.
     * @return a combined DataTableSpec.
     */
    private static DataTableSpec combineToSingleSpec(final DataTableSpec[] inSpecs) {
        return Arrays.stream(inSpecs) //
            .reduce(new DataTableSpec(), DataTableSpec::new);
    }

    /**
     * Provided an array of BufferedDataTable with equal row count and identical RowIDs it returns a joined single
     * BufferedDataTable where the columns of each BufferedDataTable are appended in respective order.
     *
     * @param inData an array of BufferedDataTable.
     * @param exec the execution context.
     * @return a combined BufferedDataTable containing all the columns from all tables.
     * @throws CanceledExecutionException - If the execution is canceled by the user
     */
    private static BufferedDataTable joinTablesWithIdenticalKeys(final BufferedDataTable[] inData,
        final ExecutionContext exec) throws CanceledExecutionException {
        DataTableSpec[] outTableSpec = createUniqueSpecs(getSpecFromInput(inData));
        BufferedDataTable out = null;
        for (int i = 1; i < inData.length; i++) {
            final BufferedDataTable uniqufiedcolNamesTable = exec.createSpecReplacerTable(inData[i], outTableSpec[i]);
            if (i == 1) {
                out = exec.createJoinedTable(inData[0], uniqufiedcolNamesTable, exec);
            } else {
                out = exec.createJoinedTable(out, uniqufiedcolNamesTable, exec);
            }
        }
        return out;
    }

    /**
     * Provided an array of BufferedDataTable, it returns a joined single BufferedDataTable where the columns of each
     * BufferedDataTable are appended in respective order.
     *
     * @param inData an array of BufferedDataTable.
     * @param exec the execution context.
     * @return a combined BufferedDataTable containing all the columns from all tables.
     * @throws InterruptedException - If the execution is canceled by the user
     * @throws CanceledExecutionException - If the execution is canceled by the user
     */
    private BufferedDataTable createNewTable(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws InterruptedException, CanceledExecutionException {

        final long[] rowCounts = Arrays.stream(inData).mapToLong(BufferedDataTable::size).toArray();

        BufferedDataContainer outDataContainer =
            exec.createDataContainer(combineToSingleSpec(createUniqueSpecs((getSpecFromInput(inData)))));

        /* Get the index of a table that is deciding the length/rowID of the result */
        int decidingTblIdx = getDecidingTableIndex();
        if (isSelectedMode(RowKeyMode.GENERATE)) {
            decidingTblIdx = IntStream.range(0, m_numInPorts) //
                .reduce((a, b) -> rowCounts[a] < rowCounts[b] ? b : a) //
                .getAsInt(); // This works because m_numInPorts is always >= 2.
        }

        MultipleRowIterators iter =
            new MultipleRowIterators(inData, decidingTblIdx, isSelectedMode(RowKeyMode.IDENTICAL));

        compute(exec, outDataContainer::addRowToTable, iter, rowCounts[decidingTblIdx]);
        outDataContainer.close();
        return outDataContainer.getTable();
    }

    /**
     * Combines the rows to create new table.
     *
     * @param exec The execution context.
     * @param output the row consumer
     * @param iters a wrapped array of RowIterators.
     * @param numRowsTotal the total number of rows at the output table.
     * @throws InterruptedException - If the execution is canceled by the user
     * @throws CanceledExecutionException - If the execution is canceled by the user
     */
    private void compute(final ExecutionContext exec, final RowConsumer output, final MultipleRowIterators iters,
        final long numRowsTotal) throws InterruptedException, CanceledExecutionException {
        while (iters.hasNext()) {
            if (numRowsTotal != -1) {
                final long currRowIndex = iters.getCurrentRowIndex();
                exec.setProgress(currRowIndex / (double)numRowsTotal,
                    () -> "Appending columns (row " + currRowIndex + "/" + numRowsTotal + ")");
            }
            exec.checkCanceled();
            output.consume(iters.next());
        }
        differingTableSizeMsg(iters);
    }

    /**
     * A helper method that checks if the configured mode equals the provided mode.
     *
     * @param toCheck the mode to check if it equals the configured mode
     * @return true if configured equals provided mode
     */
    private final boolean isSelectedMode(final RowKeyMode toCheck) {
        return RowKeyMode.valueOf(m_rowIDModeSettings.getStringValue()) == toCheck;
    }

    /**
     * A helper method that gets the selected table index used for getting row keys.
     *
     * @return the selected table index, if keys are taken from a selected table, -1 otherwise.
     */
    private final int getDecidingTableIndex() {
        if (isSelectedMode(RowKeyMode.KEY_TABLE)) {
            return m_rowIDTableSettings.getIntValue() - 1;
        } else {
            return -1;
        }
    }

    /**
     * By checking the final status of individual RowIterators, displays detailed warning messages. Warning is issued if
     * some of the input tables are shorter or longer than the deciding table. In the case of generating rowIDs, the
     * longest table is used for comparison.
     *
     * @param iters a wrapped array of RowIterators.
     * @throws InterruptedException - If the execution is canceled by the user
     */
    private void differingTableSizeMsg(final MultipleRowIterators iters) throws InterruptedException {
        // In the case of generating rowIDs, the longest table is used for comparison
        String msgPart = " longest table";
        if (isSelectedMode(RowKeyMode.KEY_TABLE)) {
            msgPart = " selected table (Table " + (iters.m_decidingTableIdx + 1) + ")";
        }

        // Display warning about shorter tables that filled with missing values.
        if (!iters.overUsedTables().equals("")) {
            setWarningMessage("Table(s) [" + iters.overUsedTables() + "] is/are shorter than the" + msgPart
                + "! Missing values have been added accordingly.");
        }
        // Display warning about longer tables that are truncated.
        if (!iters.underUsedTables().equals("")) {
            setWarningMessage("Table(s) [" + iters.underUsedTables() + "] is/are longer than the " + msgPart
                + "! Extra rows are ignored.");
        }
    }

    /**
     * The data row iterator interface.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    private static interface DataRowIterator {

        /**
         * Returns {@code true} if the iteration has more elements.
         *
         * @return {@code true} if the iteration has more elements
         * @throws InterruptedException - If the execution has been canceled
         */
        boolean hasNext() throws InterruptedException;

        /**
         * Returns the next element in the iteration. In case that there a no more new rows a default DataRow is
         * returned.
         *
         * @return the next element in the iteration or an default DataRow
         * @throws InterruptedException - If the execution has been canceled
         */
        DataRow next() throws InterruptedException;

        /**
         * Flag indicating that {@link #next()} has been invoked though {@link #hasNext()} returned @{code false}.
         *
         * @return flag indicating that {@link #next()} has been invoked though no more elements where available
         */
        boolean isOverused();
    }

    /**
     * An implementation of custom DataRowIterator that returns a row of missing values when it is at the end.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    private static final class SimpleDataRowIterator implements DataRowIterator {

        private final RowIterator m_rowIt;

        private final int m_numCol;

        private final DefaultRow m_emptyRow;

        private boolean m_overused = false;

        SimpleDataRowIterator(final RowIterator rowIt, final int numCol) {
            m_rowIt = rowIt;
            m_numCol = numCol;
            m_emptyRow = new DefaultRow("NoRowID",
                Stream.generate(DataType::getMissingCell).limit(m_numCol).toArray(DataCell[]::new));
        }

        @Override
        public boolean hasNext() {
            return m_rowIt.hasNext();
        }

        @Override
        public DataRow next() {
            /* Next returns a row of missing value cells if iterator is at the end. */
            if (!m_rowIt.hasNext()) {
                m_overused = true;
                return m_emptyRow;
            }
            return m_rowIt.next();
        }

        @Override
        public boolean isOverused() {
            return m_overused;
        }
    }

    /**
     * An implementation (streaming variant) of custom DataRowIterator that returns a row of missing values when it is
     * at the end.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    private static final class StreamableDataRowIterator implements DataRowIterator {

        private final RowInput m_rowInput;

        private DataRow m_row = null;

        private final int m_numCol;

        private final DefaultRow m_emptyRow;

        private boolean m_overused = false;

        StreamableDataRowIterator(final RowInput rowInput, final int numCol) {
            m_rowInput = rowInput;
            m_numCol = numCol;
            m_emptyRow = new DefaultRow("NoRowID",
                Stream.generate(DataType::getMissingCell).limit(m_numCol).toArray(DataCell[]::new));
        }

        @Override
        public boolean hasNext() throws InterruptedException {
            /* If hasNext() is called multiple times without calling next() in between,
             this if-clause ensures that it still returns true. */
            if (m_row == null) {
                m_row = m_rowInput.poll();
            }
            return m_row != null;
        }

        @Override
        public DataRow next() throws InterruptedException {
            /* next returns a row of missing value cells if iterator is at the end. */
            if (!hasNext()) {
                m_row = m_emptyRow;
                m_overused = true;
            }
            final DataRow row = m_row;
            m_row = null;
            return row;
        }

        @Override
        public boolean isOverused() {
            return m_overused;
        }
    }

    /**
     * A wrapper class with an array of DataRowIterator and convenience methods to check their status.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    private static final class MultipleRowIterators {

        private final DataRowIterator[] m_iterators;

        private final int m_iterCount;

        private final int m_decidingTableIdx;

        private final boolean m_identicalKeys;

        private long m_currentPos;

        /**
         * Constructor.
         *
         * @param rowIterators an array of row iterators.
         * @param keyMode the way to handle the row keys.
         * @param decidingTableIdx the table deciding the the rowID and output table length
         */
        private MultipleRowIterators(final DataRowIterator[] rowIterators, final int decidingTableIdx,
            final boolean identicalKeys) {
            m_iterators = rowIterators;
            m_iterCount = rowIterators.length;
            m_decidingTableIdx = decidingTableIdx;
            m_identicalKeys = identicalKeys;
            m_currentPos = 0;
        }

        private MultipleRowIterators(final RowInput[] rowInArray, final int decidingTblIdx,
            final boolean identicalKeys) {
            this(IntStream.range(0, rowInArray.length)//
                .mapToObj(
                    i -> new StreamableDataRowIterator(rowInArray[i], rowInArray[i].getDataTableSpec().getNumColumns()))//
                .toArray(DataRowIterator[]::new)//
                , decidingTblIdx, identicalKeys);
        }

        private MultipleRowIterators(final BufferedDataTable[] inData, final int decidingTblIdx,
            final boolean identicalKeys) {
            this(IntStream.range(0, inData.length)//
                .mapToObj(i -> new SimpleDataRowIterator(inData[i].iterator(), inData[i].getSpec().getNumColumns()))//
                .toArray(DataRowIterator[]::new)//
                , decidingTblIdx, identicalKeys);
        }

        private boolean hasNext() throws InterruptedException {
            boolean decidingTableHasNext = false;
            int count = 0;
            /* all iterators must call hasNext(). */
            for (int i = 0; i < m_iterCount; i++) {
                if (m_iterators[i].hasNext()) {
                    count++;
                    if (i == m_decidingTableIdx) {
                        decidingTableHasNext = true;
                    }
                }
            }
            // If tables suppose to have identical keys all/none should have a next. Streaming case only.
            if (m_identicalKeys) {
                if (count == m_iterCount) { // All have next.
                    return true;
                } else if (count == 0) { // None has next.
                    return false;
                } else {
                    throw new IllegalArgumentException("Tables contain non-matching row counts!");
                }

            } else if (m_decidingTableIdx == -1) { // If row keys are generated 1 is enough.
                return count > 0;
            } else {
                return decidingTableHasNext;
            }
        }

        private DataRow next() throws InterruptedException {
            final ArrayList<DataCell> cells = new ArrayList<>();
            String rowKey = "";

            for (int i = 0; i < m_iterCount; i++) {
                final DataRow currRow = m_iterators[i].next();
                for (DataCell cell : currRow) {
                    cells.add(cell);
                }
                if (i == m_decidingTableIdx) {
                    rowKey = currRow.getKey().getString();
                }
                /* This is useful in the streaming case.
                   Tables are not simply joined even if identical option is selected. */
                if (m_identicalKeys) {
                    if (i == 0) {
                        rowKey = currRow.getKey().getString();
                    } else if (!rowKey.equals(currRow.getKey().getString())) {
                        throw new IllegalArgumentException("Tables contain non-matching rows or are sorted "
                            + "differently. Keys in row " + m_currentPos + " do not match: \"" + rowKey
                            + "\" (first input) vs. \"" + currRow.getKey().getString() + "\" (input " + (i + 1) + ")");
                    }
                }
            }
            // generate new row key
            if (m_decidingTableIdx == -1 && !m_identicalKeys) {
                rowKey = "Row" + m_currentPos;
            }

            final DefaultRow res = new DefaultRow(rowKey, cells);
            m_currentPos++;
            return res;
        }

        private String overUsedTables() {
            return IntStream.range(0, m_iterCount)//
                .filter(i -> m_iterators[i].isOverused())//
                .mapToObj(i -> (i + 1) + "")//
                .collect(Collectors.joining(","));
        }

        private String underUsedTables() throws InterruptedException {
            final StringJoiner j = new StringJoiner(",");
            for (int i = 0; i < m_iterCount; i++) {
                if (m_iterators[i].hasNext()) {
                    j.add("" + i + 1);
                }
            }
            return j.toString();
        }

        private long getCurrentRowIndex() {
            return m_currentPos;
        }

    }

    /**
     * Row consumer interface used to allow pushing rows to {@link BufferedDataContainer} as well as {@link RowOutput}.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    private static interface RowConsumer {
        void consume(DataRow row) throws InterruptedException;
    }

    /**
     * Different options used to decide the RowIDs during column appending.
     *
     * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
     */
    enum RowKeyMode implements ButtonGroupEnumInterface {

            IDENTICAL("Identical row keys and table lengths"), //

            GENERATE("Generate new row keys"), //

            KEY_TABLE("Use the row keys from the input table: ");

        private static final String TOOLTIP = "<html>Choose the way row keys of the output tables are decided.<br>"
            + "If \"Identical row keys and table lengths\" is chosen, all input tables<br> "
            + "should have exactly the same row Ids in the exact same order.<html>";

        private final String m_text;

        RowKeyMode(final String text) {
            this.m_text = text;
        }

        @Override
        public String getText() {
            return m_text;
        }

        @Override
        public String getActionCommand() {
            return name();
        }

        @Override
        public String getToolTip() {
            return TOOLTIP;
        }

        @Override
        public boolean isDefault() {
            return this == IDENTICAL;
        }

    }

    //////////////// STREAMING FUNCTIONS ////////////////

    @Override
    public InputPortRole[] getInputPortRoles() {
        /* In-ports are non-distributed since it can't be guaranteed that the chunks at each port are of identical size. */
        return Stream.generate(() -> InputPortRole.NONDISTRIBUTED_STREAMABLE) //
            .limit(m_numInPorts) //
            .toArray(InputPortRole[]::new); //
    }

    @Override
    public OutputPortRole[] getOutputPortRoles() {
        return new OutputPortRole[]{OutputPortRole.DISTRIBUTED};
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {
            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                final RowInput[] rowInArray = new RowInput[inputs.length];
                final int decidingTblIdx = getDecidingTableIndex();
                /* Number of rows will be -1 for all inputs in case of streaming. */
                final long[] rowCounts = new long[m_numInPorts];
                Arrays.fill(rowCounts, -1);
                final MultipleRowIterators multiIters =
                    new MultipleRowIterators(rowInArray, decidingTblIdx, isSelectedMode(RowKeyMode.IDENTICAL));
                final RowOutput rowOut = (RowOutput)outputs[0];
                compute(exec, rowOut::push, multiIters, -1);

                for (int i = 0; i < inputs.length; i++) {
                    rowInArray[i].close();
                }
                rowOut.close();
            }
        };
    }

}
