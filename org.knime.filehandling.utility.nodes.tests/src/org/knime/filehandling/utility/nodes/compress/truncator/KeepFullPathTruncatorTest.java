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
 *   Feb 16, 2021 (Mark Ortmann, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.filehandling.utility.nodes.compress.truncator;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.knime.filehandling.utility.nodes.compress.truncator.impl.KeepFullPathTruncator;

/**
 * Tests the correctness of the {@link KeepFullPathTruncator}.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 */
public class KeepFullPathTruncatorTest {

    private static String getArchiveEntryName(final Path baseFolder, final Path path, final boolean flattenHierarchy) {
        return TruncatePathOption.KEEP_FULL_PATH.createPathTruncator(flattenHierarchy, "").truncate(baseFolder, path);
    }

    /**
     * Tests the correctness for all inputs generated by the framework.
     */
    @Test
    public void testArchiveEntryName() {
        final Path root = TruncatorTestsUtils.getRoot();
        standardTests(root);
        rootTests(root);
        curParentDirTests(root);
        baseNullTests(root);
    }

    private static void standardTests(final Path root) {
        final Path relBase = Paths.get("foo", "bar");
        final Path absBase = relBase.toAbsolutePath();
        final Path relPath = TruncatorTestsUtils.append(relBase, "subfolder", "file.txt");
        final Path absPath = relPath.toAbsolutePath();

        // relative no flattening
        final String relResult = getArchiveEntryName(relBase, relPath, false);
        final Path relExpected = Paths.get("foo", "bar", "subfolder", "file.txt");
        assertEquals(relExpected.toString(), relResult);

        // absolute no flattening
        final String absResult = getArchiveEntryName(absBase, absPath, false);
        final Path absExpected = relExpected.toAbsolutePath();
        assertEquals(root.relativize(absExpected).toString(), absResult);

        // relative flattening
        final String flatRelResult = getArchiveEntryName(relBase, relPath, true);
        final Path flatRelExpected = Paths.get("foo", "bar", "file.txt");
        assertEquals(flatRelExpected.toString(), flatRelResult);

        // absolute flattening
        final String flatAbsResult = getArchiveEntryName(absBase, absPath, true);
        final Path flatAbsExpected = flatRelExpected.toAbsolutePath();
        assertEquals(root.relativize(flatAbsExpected).toString(), flatAbsResult);

        final String relBaseEqualsPathExpected = relBase.toString();
        assertEquals(relBaseEqualsPathExpected, getArchiveEntryName(relBase, relBase, false));
        assertEquals(relBaseEqualsPathExpected, getArchiveEntryName(relBase, relBase, true));

        final String absBaseEqualsPathExpected = root.relativize(absBase).toString();
        assertEquals(absBaseEqualsPathExpected, getArchiveEntryName(absBase, absBase, false));
        assertEquals(absBaseEqualsPathExpected, getArchiveEntryName(absBase, absBase, true));

    }

    private static void rootTests(final Path root) {
        final Path file = TruncatorTestsUtils.append(root, "subfolder", "file.txt");
        final String absRootRes = getArchiveEntryName(root, file, false);
        assertEquals(root.relativize(file).toString(), absRootRes);

        final String relRootRes = getArchiveEntryName(root.relativize(root), root.relativize(file), false);
        assertEquals(root.relativize(file).toString(), relRootRes);

        final Path flatExpected = TruncatorTestsUtils.append(root, "file.txt");
        final String flatAbsRootRes = getArchiveEntryName(root, file, true);
        assertEquals(root.relativize(flatExpected).toString(), flatAbsRootRes);
        final String flatRelRootRes = getArchiveEntryName(root.relativize(root), root.relativize(file), true);
        assertEquals(root.relativize(flatExpected).toString(), flatRelRootRes);
    }

    private static void curParentDirTests(final Path root) {
        final Path base = TruncatorTestsUtils.append(root, "subfolder", "foo", "..");
        final Path file = TruncatorTestsUtils.append(base, "bar", ".", "file.txt", "..", "file.txt", ".");
        final String absRootRes = getArchiveEntryName(base, file, false);

        assertEquals(root.relativize(file.normalize()).toString(), absRootRes);

        final String relRootRes = getArchiveEntryName(base.relativize(root), root.relativize(file), false);
        assertEquals(root.relativize(file.normalize()).toString(), relRootRes);

        final Path flatExpected = root.relativize(TruncatorTestsUtils.append(root, "subfolder", "file.txt"));
        final String flatAbsRootRes = getArchiveEntryName(base, file, true);
        assertEquals(flatExpected.toString(), flatAbsRootRes);
        final String flatRelRootRes = getArchiveEntryName(root.relativize(base), root.relativize(file), true);
        assertEquals(flatExpected.toString(), flatRelRootRes);
    }

    private static void baseNullTests(final Path root) {
        final Path relPath = Paths.get("foo", "bar", "subfolder", "file.txt");
        final String relExpected = relPath.toString();
        final String flatExpected = "file.txt";
        assertEquals(relExpected, getArchiveEntryName(null, relPath, false));
        assertEquals(flatExpected, getArchiveEntryName(null, relPath, true));

        final Path absPath = relPath.toAbsolutePath();
        final String absExpected = root.relativize(absPath).toString();

        assertEquals(absExpected, getArchiveEntryName(null, absPath, false));
        assertEquals(flatExpected, getArchiveEntryName(null, absPath, true));
    }

}