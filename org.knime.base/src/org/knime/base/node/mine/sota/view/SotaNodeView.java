/* 
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
 *   Nov 16, 2005 (Kilian Thiel): created
 */
package org.knime.base.node.mine.sota.view;

import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.knime.base.node.mine.sota.SotaNodeModel;
import org.knime.core.node.NodeView;

/**
 * 
 * @author Kilian Thiel, University of Konstanz
 */
public class SotaNodeView extends NodeView {
    private SotaDrawingPane m_pane;

    private SotaTreeViewPropsPanel m_panel;

    private JPanel m_outerPanel;

    /**
     * Constructor of SotaNodeView. Creates new instance of SotaNodeView with
     * given SotaNodeModel.
     * 
     * @param nodeModel the node model
     */
    public SotaNodeView(final SotaNodeModel nodeModel) {
        super(nodeModel);

        // get data model, init view
        m_pane = new SotaDrawingPane(nodeModel.getSotaManager().getRoot(),
                nodeModel.getSotaManager().getInDataContainer(), nodeModel
                        .getSotaManager().getOriginalData(), nodeModel
                        .getSotaManager().isUseHierarchicalFuzzyData(),
                nodeModel.getSotaManager().getMaxHierarchicalLevel());

        nodeModel.getInHiLiteHandler(0).addHiLiteListener(m_pane);
        m_pane.setHiliteHandler(nodeModel.getInHiLiteHandler(0));

        m_panel = new SotaTreeViewPropsPanel(m_pane);

        getJMenuBar().add(m_pane.createHiLiteMenu());
        getJMenuBar().add(m_panel.createZoomMenu());

        m_outerPanel = new JPanel();
        m_outerPanel.setLayout(new BoxLayout(m_outerPanel, BoxLayout.Y_AXIS));
        m_outerPanel.add(m_panel);
        super.setShowNODATALabel(false);
        super.setComponent(m_outerPanel);
    }

    /**
     * @see org.knime.core.node.NodeView#modelChanged()
     */
    @Override
    protected void modelChanged() {
        SotaNodeModel node = (SotaNodeModel)this.getNodeModel();

        m_pane.setRoot(node.getSotaManager().getRoot());
        m_pane.setData(node.getSotaManager().getInDataContainer());
        m_pane.setMaxHLevel(node.getSotaManager().getMaxHierarchicalLevel());

        m_pane.modelChanged(true);
        m_panel.modelChanged();
    }

    /**
     * @see org.knime.core.node.NodeView#onClose()
     */
    @Override
    protected void onClose() {
    }

    /**
     * @see org.knime.core.node.NodeView#onOpen()
     */
    @Override
    protected void onOpen() {
    }
}
