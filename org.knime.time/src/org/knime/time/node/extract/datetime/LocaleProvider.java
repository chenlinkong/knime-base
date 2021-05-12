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
 *   May 11, 2021 (ortmann): created
 */
package org.knime.time.node.extract.datetime;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

/**
 *
 * @author ortmann
 */
enum LocaleProvider {

        JAVA_8 {

            private final Locale[] m_locales = Arrays.stream(Locale.getAvailableLocales())//
                // with java 11 new Locales without region are available that need to be filtered plus since we use the
                // languageTag to save and load the Locales we cannot load Locales that have a variant, e.g., no_NO_NY != no_NO
                .filter(l -> ExtractDateTimeFieldsNodeModel.LOCALE_MAPPING.containsKey(l.toLanguageTag()) //
                    || (!l.getCountry().isEmpty()//
                        && l.getVariant().isEmpty()))//
                .sorted()//
                .toArray(Locale[]::new);

            @Override
            Locale[] getLocales() {
                return m_locales;
            }

            @Override
            String localeToString(final Locale locale) {
                return locale.toLanguageTag();
            }

        },

        JAVA_11 {

            private final Locale[] m_locales = Arrays.stream(Locale.getAvailableLocales())//
                .filter(l -> !l.getCountry().isEmpty())//
                .toArray(Locale[]::new);

            @Override
            Locale[] getLocales() {
                return m_locales;
            }

            @Override
            String localeToString(final Locale locale) {
                return locale.toString();
            }

        };

    /**
     * Returns the Locales that can be selected by the user.
     *
     * @return the selectable locales
     */
    abstract Locale[] getLocales();

    /**
     * Converts a locale to the string that is used to save it.
     *
     * @param locale the locale to be converted
     * @return the string representation of this locale
     */
    abstract String localeToString(final Locale locale);

    /**
     * Inverses the transformation done via {@link #localeToString(Locale)}.
     *
     * @param string the string representation of a Locale
     * @return the Locale associated with the given string
     */
    Optional<Locale> stringToLocale(final String string) {
        return Arrays.stream(getLocales())//
            .filter(l -> localeToString(l).equals(string))//
            .findFirst();
    }
}
