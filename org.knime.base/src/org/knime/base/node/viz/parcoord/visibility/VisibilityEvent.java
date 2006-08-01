/* @(#)$RCSfile$ 
 * $Revision$ $Date$ $Author$
 * 
 * -------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2006
 * University of Konstanz, Germany.
 * Chair for Bioinformatics and Information Mining
 * Prof. Dr. Michael R. Berthold
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any quesions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * -------------------------------------------------------------------
 * 
 * History
 *   Jun 27, 2005 (pintilie): created
 */
package org.knime.base.node.viz.parcoord.visibility;

import java.util.EventObject;
import java.util.HashSet;
import java.util.Set;

import org.knime.core.data.DataCell;



/**
 * 
 * @author pintilie, University of Konstanz
 */
public class VisibilityEvent extends EventObject {

    private final HashSet<DataCell> m_keys;
    
    /** 
     * Creates a new event with the underlying source and one data cell.
     * @param  src The object on which the event initially occurred.
     * @param  key A <code>DataCell</code> for which this event is created.
     * @throws NullPointerException If the key is <code>null</code>.
     * @throws IllegalArgumentException If the source is <code>null</code>.
     * 
     * @see java.util.EventObject#EventObject(Object)
     */
    public VisibilityEvent(final Object src, final DataCell key) {
        super(src);
        m_keys = new HashSet<DataCell>();
        m_keys.add(key);
    }
    
    /** 
     * Creates a new event with the underlying source and a set of row keys.
     * @param  src  The object on which the event initially occurred.
     * @param  keys A set of <code>DataCell</code> row keys for which the 
     *         event is created.
     * @throws NullPointerException If keys are <code>null</code>.
     * @throws IllegalArgumentException If the source is <code>null</code>.
     *
     * @see java.util.EventObject#EventObject(Object)
     */
    public VisibilityEvent(final Object src, final Set<DataCell> keys) {
        super(src);
        m_keys = new HashSet<DataCell>(keys);
    }

    /** 
     * Returns the set of <code>DataCell</code> row keys on which the event 
     * initially occured.
     * @return A set of row keys.
     */
    public Set<DataCell> keys() { 
        return m_keys;
    }
}
