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
 *   May 10, 2021 (Laurin Siefermann, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.filehandling.core.defaultnodesettings.filtermode;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.knime.filehandling.core.defaultnodesettings.filtermode.FileAndFolderFilter.FilterType;

/**
 *
 * @author Laurin Siefermann, KNIME GmbH, Konstanz, Germany
 */
public class FileAndFolderFilterTest {

    private Path m_tempDir;

    private Path m_FilePath;

    private Path m_bigFilePath;

    private Path m_pngFilePath;

    private Path m_exclFilePath;

    private Path m_firstFolderPath;

    private Path m_secondFolderPath;

    private Path m_thirdFolderPath;

    private Path m_fourthFolderPath;

    private List<Path> m_allPaths;

    /**
     * Initializes the directory structure used for testing.
     *
     * @throws IOException
     */
    @Before
    public void initializeTestDirectory() throws IOException {
        m_tempDir = Files.createTempDirectory("file_chooser_path_accessor_test");
        m_FilePath = m_tempDir.resolve("file.txt");
        m_bigFilePath = m_tempDir.resolve("FILE.PNG");
        m_pngFilePath = m_tempDir.resolve("pngFile.png");
        // hidden file
        m_exclFilePath = m_tempDir.resolve(".exclFile.xlsx");
        m_firstFolderPath = m_tempDir.resolve("firstFolder");
        m_secondFolderPath = m_tempDir.resolve("secondFOLDER");
        m_thirdFolderPath = m_tempDir.resolve(".thirdFol");
        m_fourthFolderPath = m_tempDir.resolve("fourth");

        Files.createFile(m_FilePath);
        Files.createFile(m_bigFilePath);
        Files.createFile(m_pngFilePath);
        Files.createFile(m_exclFilePath);
        String os = System.getProperty("os.name");
        if (os.startsWith("Windows")) {
            //set hidden attribute for windows
            Files.setAttribute(m_exclFilePath, "dos:hidden", true, LinkOption.NOFOLLOW_LINKS);
        }

        Files.createDirectory(m_firstFolderPath);
        Files.createDirectory(m_secondFolderPath);
        Files.createDirectory(m_thirdFolderPath);
        Files.createDirectory(m_fourthFolderPath);
        if (os.startsWith("Windows")) {
            //set hidden attribute for windows
            Files.setAttribute(m_thirdFolderPath, "dos:hidden", true, LinkOption.NOFOLLOW_LINKS);
        }

        m_allPaths = new ArrayList<>();
        m_allPaths.add(m_FilePath);
        m_allPaths.add(m_bigFilePath);
        m_allPaths.add(m_pngFilePath);
        m_allPaths.add(m_exclFilePath);
        m_allPaths.add(m_firstFolderPath);
        m_allPaths.add(m_secondFolderPath);
        m_allPaths.add(m_thirdFolderPath);
        m_allPaths.add(m_fourthFolderPath);
    }

    ///////////////////////////////
    //   Tests regarding files   //
    ///////////////////////////////

    /**
     * <pre>
     * Filter for .png extension, case-insensitive (file, FILE = same)
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: file.txt
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testFileExtension() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFilesByExtension(true);
        filterOptionsSettings.setFilesExtensionExpression("png");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 1, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /**
     * <pre>
     * Filter for .png extension, case-sensitive (file, FILE = different)
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: file.txt, FILE.PNG
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testFileExtensionCaseSensitive() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFilesByExtension(true);
        filterOptionsSettings.setFilesExtensionExpression("png");
        filterOptionsSettings.setFilesExtensionCaseSensitive(true);

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 2, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /**
     * <pre>
     * Filter for file*, case-insensitive (file, FILE = same)
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: pngFile.PNG
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testWildcardFileName() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFilesByName(true);
        filterOptionsSettings.setFilesNameExpression("file*");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 1, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /**
     * <pre>
     * Filter for file*, case-sensitive (file, FILE = different)
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: FILE.PNG, pngFile.png
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testWildcardFileNameCaseSensitive() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilesNameCaseSensitive(true);
        filterOptionsSettings.setFilterFilesByName(true);
        filterOptionsSettings.setFilesNameExpression("file*");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 2, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /**
     * <pre>
     * Filter for (file)+.\\w+ , case-insensitive (file, FILE = same)
     *
     * Regex: "file" (case-insensitive) followed by "." followed by one or more word characters
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: pngFile.PNG
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testRegexFileName() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFilesByName(true);
        filterOptionsSettings.setFilesNameFilterType(FilterType.REGEX);
        filterOptionsSettings.setFilesNameExpression("(file)+.\\w+");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 1, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /**
     * <pre>
     * Filter for (file)+.\\w+ , case-sensitive (file, FILE = different)
     *
     * Regex: "file" (case-sensitive) followed by "." followed by one or more word characters
     *
     * Input: file.txt, FILE.PNG, pngFile.png, .exclFile.xlsx (hidden)
     * Filtered files: FILE.PNG, pngFile.png
     * Filtered hidden files: .exclFile.xlsx (hidden)
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testRegexFileNameCaseSensitive() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFilesByName(true);
        filterOptionsSettings.setFilesNameCaseSensitive(true);
        filterOptionsSettings.setFilesNameFilterType(FilterType.REGEX);
        filterOptionsSettings.setFilesNameExpression("(file)+.\\w+");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 2, filter.getNumberOfFilteredFiles());

        filterOptionsSettings.setIncludeHiddenFiles(true);
        test(filter);
        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFiles());
    }

    /////////////////////////////////
    //   Tests regarding folders   //
    /////////////////////////////////

    //TODO: hidden not working right now.

    /**
     * <pre>
     * Filter for *Folder, case-insensitive (Folder, FOLDER = same)
     *
     * Input: firstFolder, secondFOLDER, .thirdFol (hidden), fourth
     * Filtered folders: fourth, .thirdFol (hidden)
     * Filtered hidden folders:
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testWildcardFolderName() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFoldersByName(true);
        filterOptionsSettings.setFoldersNameExpression("*Folder");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 2, filter.getNumberOfFilteredFolders());

        //        filterOptionsSettings.setIncludeHiddenFolders(true);
        //        test(filter);
        //        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFolders());
    }

    /**
     * <pre>
     * Filter for *Folder, case-sensitive (file, FILE = different)
     *
     * Input: firstFolder, secondFOLDER, .thirdFol (hidden), fourth
     * Filtered folders: secondFOLDER, .thirdFol (hidden), fourth
     * Filtered hidden folders:
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testWildcardFolderNameCaseSensitive() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFoldersByName(true);
        filterOptionsSettings.setFoldersNameExpression("*Folder");
        filterOptionsSettings.setFoldersNameCaseSensitive(true);

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 3, filter.getNumberOfFilteredFolders());

        //        filterOptionsSettings.setIncludeHiddenFolders(true);
        //        testAllFiles(filter);
        //        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFolders());
    }

    /**
     * <pre>
     * Filter for \\w+Folder , case-sensitive (Folder, FOLDER = different)
     *
     * Regex: one or more word characters followed by "Folder" (case-insensitive)
     *
     * Input: firstFolder, secondFOLDER, .thirdFol (hidden), fourth
     * Filtered folders: .thirdFol (hidden), fourth
     * Filtered hidden folders:
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testRegexFolderName() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFoldersByName(true);
        filterOptionsSettings.setFoldersNameFilterMode(FilterType.REGEX);
        filterOptionsSettings.setFoldersNameExpression("\\w+Folder");

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 2, filter.getNumberOfFilteredFolders());

        //        filterOptionsSettings.setIncludeHiddenFolders(true);
        //        testAllFiles(filter);
        //        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFolders());
    }

    /**
     * <pre>
     * Filter for \\w+Folder , case-sensitive (Folder, FOLDER = different)
     *
     * Regex: one or more word characters followed by "Folder" (case-sensitive)
     *
     * Input: firstFolder, secondFOLDER, .thirdFol (hidden), fourth
     * Filtered folders: secondFOLDER, .thirdFol (hidden), fourth
     * Filtered hidden folders:
     * </pre>
     *
     * @throws IOException
     */
    @Test
    public void testRegexFolderNameCaseSensitive() throws IOException {
        FilterOptionsSettings filterOptionsSettings = new FilterOptionsSettings();
        filterOptionsSettings.setFilterFoldersByName(true);
        filterOptionsSettings.setFoldersNameFilterMode(FilterType.REGEX);
        filterOptionsSettings.setFoldersNameExpression("\\w+Folder");
        filterOptionsSettings.setFoldersNameCaseSensitive(true);

        FileAndFolderFilter filter = new FileAndFolderFilter(m_tempDir, filterOptionsSettings);
        test(filter);
        assertEquals("normal", 3, filter.getNumberOfFilteredFolders());

        //        filterOptionsSettings.setIncludeHiddenFolders(true);
        //        testAllFiles(filter);
        //        assertEquals("hidden", 1, filter.getNumberOfFilteredHiddenFolders());
    }

    private void test(final FileAndFolderFilter filter) throws IOException {
        for (int i = 0; i < m_allPaths.size(); i++) {
            Path filePath = m_allPaths.get(i);

            BasicFileAttributes attrs = Files.readAttributes(filePath, BasicFileAttributes.class);

            filter.test(filePath, attrs);
        }
    }

}