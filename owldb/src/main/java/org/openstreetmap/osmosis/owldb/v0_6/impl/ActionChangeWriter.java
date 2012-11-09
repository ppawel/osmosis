// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.sql.SQLException;

import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityProcessor;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;


/**
 * Writes entities to a database according to a specific action.
 * 
 * @author Brett Henderson
 */
public class ActionChangeWriter implements EntityProcessor {
	private ChangeWriter changeWriter;
	private ChangeAction action;


	/**
	 * Creates a new instance.
	 * 
	 * @param changeWriter
	 *            The underlying change writer.
	 * @param action
	 *            The action to apply to all writes.
	 */
	public ActionChangeWriter(ChangeWriter changeWriter, ChangeAction action) {
		this.changeWriter = changeWriter;
		this.action = action;
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(BoundContainer bound) {
		// Do nothing.
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(NodeContainer nodeContainer) {
		try {
			changeWriter.write(nodeContainer.getEntity(), action);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(WayContainer wayContainer) {
		try {
			changeWriter.write(wayContainer.getEntity(), action);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(RelationContainer relationContainer) {
		try {
			changeWriter.write(relationContainer.getEntity(), action);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}