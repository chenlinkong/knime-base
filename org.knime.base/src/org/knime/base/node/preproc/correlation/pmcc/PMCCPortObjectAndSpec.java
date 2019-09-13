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
 *
 * History
 *   18.02.2007 (wiswedel): created
 */
package org.knime.base.node.preproc.correlation.pmcc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.swing.JComponent;

import org.knime.base.util.HalfDoubleMatrix;
import org.knime.base.util.HalfIntMatrix;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataColumnProperties;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.renderer.DataValueRenderer;
import org.knime.core.data.renderer.DoubleValueRenderer.FullPrecisionRendererFactory;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContent;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectSpecZipInputStream;
import org.knime.core.node.port.PortObjectSpecZipOutputStream;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.util.CheckUtils;

/**
 * PortObject and PortObjectSpec of the model that's passed between the
 * correlation nodes.
 * @author Bernd Wiswedel, University of Konstanz
 */
public final class PMCCPortObjectAndSpec implements PortObject, PortObjectSpec {

    /**
     * Serializer for {@link PMCCPortObjectAndSpec}s.
     *
     * @noreference This class is not intended to be referenced by clients.
     * @since 3.0
     */
    public static final class ModelSerializer extends PortObjectSerializer<PMCCPortObjectAndSpec> {
        @Override
        public PMCCPortObjectAndSpec loadPortObject(
                final PortObjectZipInputStream in,
                final PortObjectSpec spec,
                final ExecutionMonitor exec) throws IOException,
                CanceledExecutionException {
            return (PMCCPortObjectAndSpec)spec;
        }

        @Override
        public void savePortObject(final PMCCPortObjectAndSpec portObject,
                final PortObjectZipOutputStream out,
                final ExecutionMonitor exec)
                throws IOException, CanceledExecutionException {
        }
    }

    /**
     * Serializer for {@link PMCCPortObjectAndSpec}s.
     *
     * @noreference This class is not intended to be referenced by clients.
     * @since 3.0
     */
    public static final class SpecSerializer extends PortObjectSpecSerializer<PMCCPortObjectAndSpec> {
        @Override
        public PMCCPortObjectAndSpec loadPortObjectSpec(
                final PortObjectSpecZipInputStream in) throws IOException {
            ModelContentRO cont = ModelContent.loadFromXML(in);
            try {
                return load(cont);
            } catch (InvalidSettingsException | IllegalArgumentException e) {
                throw new IOException("Can't parse content", e);
            }
        }

        @Override
        public void savePortObjectSpec(
                final PMCCPortObjectAndSpec portObjectSpec,
                final PortObjectSpecZipOutputStream out)
            throws IOException {
            ModelContent cont = new ModelContent("correlation");
            portObjectSpec.save(cont);
            cont.saveToXML(out);
        }
    }

    /** Convenience access field for the port type. */
    public static final PortType TYPE = PortTypeRegistry.getInstance().getPortType(PMCCPortObjectAndSpec.class);

    private final String[] m_colNames;
    private final HalfDoubleMatrix m_correlations;
    private final HalfDoubleMatrix m_pValues;
    private final HalfIntMatrix m_degreesOfFreedom;
    private final PValueAlternative m_pValAlternative;

    /** Values smaller than this are considered to be 0, used to avoid
     * round-off errors.
     * @since 2.6*/
    public static final double ROUND_ERROR_OK = 1e-8;

    /** Creates new object, whereby no correlation values are available.
     * @param includes The columns being analyzed.
     * @noreference This constructor is not intended to be referenced by clients.
     */
    public PMCCPortObjectAndSpec(final String[] includes) {
        this(includes, null);
    }

    /** Creates new object with content. Used in the execute method.
     * @param includes The names of the columns.
     * @param cors The correlation values
     * @noreference This constructor is not intended to be referenced by clients.
     */
    public PMCCPortObjectAndSpec(final String[] includes, final HalfDoubleMatrix cors) {
        this(includes, cors, null, null, null);
    }

    /**
     * Creates new object with content.
     *
     * @param includes the names of the columns
     * @param cors the correlation values
     * @param pValues the p-values
     * @param degreesOfFreedom the degrees of freedom
     * @param pValueAlternative the kind of p-value that was computed
     * @since 4.1
     */
    public PMCCPortObjectAndSpec(final String[] includes, final HalfDoubleMatrix cors, final HalfDoubleMatrix pValues,
        final HalfIntMatrix degreesOfFreedom, final PValueAlternative pValueAlternative) {
        CheckUtils.checkArgumentNotNull(includes);
        CheckUtils.checkArgument(!Arrays.asList(includes).contains(null), "The included names must not contain null values");
        final int l = includes.length;

        // Check correlations matrix if present
        if (cors != null) {
            CheckUtils.checkArgument(cors.getRowCount() == l, "Correlations array has wrong size, expected %d got %d",
                l, cors.getRowCount());
            for (int i = 0; i < l; i++) {
                for (int j = i + 1; j < l; j++) {
                    double d = cors.get(i, j);
                    if (d < -1.0) {
                        if (d < -1.0 - 1e-4) { // this can be set more strict once bug 5455 is fixed - compare to 2.9 branch
                            NodeLogger.getLogger(getClass()).assertLog(false,
                                "Correlation measure is out of range: " + d);
                        }
                        cors.set(i, j, -1.0);
                    } else if (d > 1.0) {
                        if (d > 1.0 + 1e-4) {
                            NodeLogger.getLogger(getClass()).assertLog(false,
                                "Correlation measure is out of range: " + d);
                        }
                        cors.set(i, j, 1.0);
                    }
                }
            }
        }

        // Check other arguments
        if (pValues != null) {
            // If the p-values are given the degrees of freedom and p-value alternative must also be given
            CheckUtils.checkArgumentNotNull(degreesOfFreedom,
                "degreesOfFreedom must not be null if the p-values are given.");
            CheckUtils.checkArgumentNotNull(pValueAlternative,
                "pValueAlternative must not be null if the p-values are given.");

            CheckUtils.checkArgument(pValues.getRowCount() == l, "p-values array has wrong size, expected %d got %d", l,
                pValues.getRowCount());
            CheckUtils.checkArgument(degreesOfFreedom.getRowCount() == l,
                "degrees of freedom array has wrong size, expected %d got %d", l, degreesOfFreedom.getRowCount());
        }

        m_colNames = includes;
        m_correlations = cors;
        m_pValues = pValues;
        m_degreesOfFreedom = degreesOfFreedom;
        m_pValAlternative = pValueAlternative;
    }

    /** {@inheritDoc} */
    @Override
    public PortObjectSpec getSpec() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public String getSummary() {
        return "Correlation values on " + m_colNames.length + " columns";
    }


    /**
     * Returns a matrix with the pairwise correlation values. The entries in the
     * matrix match the entries in {@link #getColNames()}.
     * Note that this can be <code>null</code> if {@link #hasData()} returns false.
     *
     * @return a matrix with correlation values
     */
    public HalfDoubleMatrix getCorrelationMatrix() {
        return m_correlations;
    }

    /**
     * Returns a matrix with the pairwise p-values. The entries in the matrix match the entries in
     * {@link #getColNames()}.
     * Note that this can be <code>null</code> if {@link #hasPValues()} returns false.
     *
     * @return a matrix with p-values
     * @since 4.1
     */
    public HalfDoubleMatrix getPValueMatrix() {
        return m_pValues;
    }

    /**
     * Returns a matrix with the pairwise degrees of freedom. The entries in the matrix match the entries in
     * {@link #getColNames()}.
     * Note that this can be <code>null</code> if {@link #hasDegreesOfFreedom()} returns false.
     *
     * @return a matrix with degrees of freedom values
     * @since 4.1
     */
    public HalfIntMatrix getDegreesOfFreedomMatrix() {
        return m_degreesOfFreedom;
    }

    /**
     * Returns which kind of p-values were computed. This is valid for all p-values in the matrix returned by
     * {@link #getPValueMatrix()}. <br/>
     * Note that this can be <code>null</code> if {@link #hasPValues()} returns false.
     *
     * @return the p-value alternative
     * @since 4.1
     */
    public PValueAlternative getPValueAlternative() {
        return m_pValAlternative;
    }

    /** @return If correlation values are available.
     * @since 2.6*/
    public boolean hasData() {
        return m_correlations != null;
    }

    /**
     * @return if the p-values are available.
     * @since 4.1
     */
    public boolean hasPValues() {
        return m_pValues != null;
    }

    /**
     * @return if the degrees of freedom are available
     * @since 4.1
     */
    public boolean hasDegreesOfFreedom() {
        return m_degreesOfFreedom != null;
    }

    /**
     * Get set of column names that would be in the output table if a given
     * correlation threshold is applied.
     * @param threshold The threshold, in [0, 1]
     * @return The set of string suggested as "survivors"
     * @since 2.6
     */
    public String[] getReducedSet(final double threshold) {
        if (!hasData()) {
            throw new IllegalStateException("No data available");
        }
        final int l = m_colNames.length;
        boolean[] hideFlags = new boolean[l];
        int[] countsAboveThreshold = new int[l];
        HashSet<String> excludes = new HashSet<String>();
        if (Thread.currentThread().isInterrupted()) {
            return new String[0];
        }
        int bestIndex; // index of next column to include
        do {
            Arrays.fill(countsAboveThreshold, 0);
            for (int i = 0; i < l; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    return new String[0];
                }
                if (hideFlags[i]) {
                    continue;
                }
                for (int j = i + 1; j < l; j++) {
                    if (hideFlags[j]) {
                        continue;
                    }
                    double d = m_correlations.get(i, j);
                    if (Math.abs(d) >= threshold) {
                        countsAboveThreshold[i] += 1;
                        countsAboveThreshold[j] += 1;
                    }
                }
            }
            int max = -1;
            bestIndex = -1;
            for (int i = 0; i < l; i++) {
                if (hideFlags[i] || countsAboveThreshold[i] == 0) {
                    continue;
                }
                if (countsAboveThreshold[i] > max) {
                    bestIndex = i;
                    max = countsAboveThreshold[i];
                }
            }
            if (bestIndex >= 0) {
                if (Thread.currentThread().isInterrupted()) {
                    return new String[0];
                }
                for (int i = 0; i < l; i++) {
                    if (hideFlags[i] || i == bestIndex) {
                        continue;
                    }
                    double d = m_correlations.get(bestIndex, i);
                    if (i == bestIndex || Math.abs(d) >= threshold) {
                        hideFlags[i] = true;
                        excludes.add(m_colNames[i]);
                    }
                }
                hideFlags[bestIndex] = true;
            }
        } while (bestIndex >= 0);
        String[] result = new String[l - excludes.size()];
        int last = 0;
        for (int i = 0; i < l; i++) {
            if (!excludes.contains(m_colNames[i])) {
                result[last++] = m_colNames[i];
            }
        }
        return result;
    }

    /**
     * Creates the correlation table, used in the view. Prior to 4.1 the table was also used as output.
     *
     * @param con For progress info/cancelation
     * @return The correlation table
     * @throws CanceledExecutionException If canceled.
     * @since 2.6
     */
    public BufferedDataTable createCorrelationMatrix(final ExecutionContext con)
        throws CanceledExecutionException {
        BufferedDataContainer cont =
            con.createDataContainer(createOutSpec(m_colNames));
        return (BufferedDataTable)createCorrelationMatrix(cont, con);

    }

    private DataTable createCorrelationMatrix(final DataContainer cont,
            final ExecutionMonitor mon)
        throws CanceledExecutionException {
        if (!hasData()) {
            throw new IllegalStateException("No data available");
        }
        final int l = m_colNames.length;
        for (int i = 0; i < l; i++) {
            RowKey key = new RowKey(m_colNames[i]);
            DataCell[] cells = new DataCell[l];
            for (int j = 0; j < l; j++) {
                if (i == j) {
                    cells[i] = MAX_VALUE_CELL;
                } else {
                    double corr = m_correlations.get(i, j);
                    if (Double.isNaN(corr)) {
                        cells[j] = DataType.getMissingCell();
                    } else {
                        cells[j] = new DoubleCell(corr);
                    }
                }
            }
            mon.checkCanceled();
            cont.addRowToTable(new DefaultRow(key, cells));
            mon.setProgress(i / (double)l, "Added row " + i);
        }
        cont.close();
        return cont.getTable();
    }

    private static final DataCell MIN_VALUE_CELL = new DoubleCell(-1.0);
    private static final DataCell MAX_VALUE_CELL = new DoubleCell(1.0);

    /** Creates output spec for correlation table.
     * @param names the column names being analyzed.
     * @return The new output spec.
     * @since 2.6
     */
    public static DataTableSpec createOutSpec(final String[] names) {
        String descFullPrecision = new FullPrecisionRendererFactory().getDescription();
        DataColumnSpec[] colSpecs = new DataColumnSpec[names.length];
        for (int i = 0; i < colSpecs.length; i++) {
            DataColumnSpecCreator c =
                new DataColumnSpecCreator(names[i], DoubleCell.TYPE);
            Map<String, String> properties = new HashMap<>(1);
            properties.put(DataValueRenderer.PROPERTY_PREFERRED_RENDERER, descFullPrecision);
            c.setProperties(new DataColumnProperties(properties));
            c.setDomain(new DataColumnDomainCreator(
                    MIN_VALUE_CELL, MAX_VALUE_CELL).createDomain());
            colSpecs[i] = c.createSpec();
        }
        return new DataTableSpec("Correlation values", colSpecs);
    }

    private static final String CFG_INTERNAL = "pmcc_model";
    private static final String CFG_NAMES = "names";
    private static final String CFG_VALUES = "correlation_values";
    private static final String CFG_CONTAINS_VALUES = "contains_values";
    private static final String CFG_P_VALUES = "p_values";
    private static final String CFG_CONTAINS_P_VALUES = "contains_p_values";
    private static final String CFG_DEGREES_OF_FREEDOM = "degrees_of_freedom";
    private static final String CFG_CONTAINS_DEGREES_OF_FREEDOM = "contains_degrees_of_freedom";
    private static final String CFG_P_VALUE_ALTERNATIVE = "p_value_alternative";
    private static final String CFG_CONTAINS_P_VALUE_ALTERNATIVE = "contains_p_value_alternative";

    /** Saves this object to a config.
     * @param m To save to.
     */
    public void save(final ConfigWO m) {
        ConfigWO sub = m.addConfig(CFG_INTERNAL);
        // Included names
        sub.addStringArray(CFG_NAMES, m_colNames);
        // Correlation matrix
        sub.addBoolean(CFG_CONTAINS_VALUES, m_correlations != null);
        if (m_correlations != null) {
            m_correlations.save(sub.addConfig(CFG_VALUES));
        }
        // P-values
        sub.addBoolean(CFG_CONTAINS_P_VALUES, m_pValues != null);
        if (m_pValues != null) {
            m_pValues.save(sub.addConfig(CFG_P_VALUES));
        }
        // Degrees of freedom
        sub.addBoolean(CFG_CONTAINS_DEGREES_OF_FREEDOM, m_degreesOfFreedom != null);
        if (m_degreesOfFreedom != null) {
            m_degreesOfFreedom.save(sub.addConfig(CFG_DEGREES_OF_FREEDOM));
        }
        // P-Value alternative
        sub.addBoolean(CFG_CONTAINS_P_VALUE_ALTERNATIVE, m_pValAlternative != null);
        if (m_pValAlternative != null) {
            sub.addString(CFG_P_VALUE_ALTERNATIVE, m_pValAlternative.name());
        }
    }

    /** Factory method to load from config.
     * @param m to load from.
     * @return new object loaded from argument
     * @throws InvalidSettingsException If that fails.
     * @throws IllegalArgumentException If the saved values are inconsistent
     */
    public static PMCCPortObjectAndSpec load(final ConfigRO m)
        throws InvalidSettingsException {
        final ConfigRO sub = m.getConfig(CFG_INTERNAL);
        // Included names
        final String[] includes = sub.getStringArray(CFG_NAMES);
        // Correlation matrix
        HalfDoubleMatrix cors = null;
        if (sub.getBoolean(CFG_CONTAINS_VALUES)) {
            cors =
                new HalfDoubleMatrix(sub.getConfig(CFG_VALUES));
        }
        // P-values
        HalfDoubleMatrix pValues = null;
        if (sub.getBoolean(CFG_CONTAINS_P_VALUES, false)) {
            pValues = new HalfDoubleMatrix(sub.getConfig(CFG_P_VALUES));
        }
        // Degrees of freedom
        HalfIntMatrix degreesOfFreedom = null;
        if (sub.getBoolean(CFG_CONTAINS_DEGREES_OF_FREEDOM, false)) {
            degreesOfFreedom = new HalfIntMatrix(sub.getConfig(CFG_DEGREES_OF_FREEDOM));
        }
        // P-Value alternative
        PValueAlternative pValueAlternative = null;
        if (sub.getBoolean(CFG_CONTAINS_P_VALUE_ALTERNATIVE, false)) {
            pValueAlternative = PValueAlternative.valueOf(sub.getString(CFG_P_VALUE_ALTERNATIVE));
        }
        // The values are checked in the constructor
        return new PMCCPortObjectAndSpec(includes, cors, pValues, degreesOfFreedom, pValueAlternative);

    }

    /**
     * @return the colNames
     */
    public String[] getColNames() {
        return m_colNames;
    }

    /** {@inheritDoc} */
    @Override
    public JComponent[] getViews() {
        return null;
    }
}
