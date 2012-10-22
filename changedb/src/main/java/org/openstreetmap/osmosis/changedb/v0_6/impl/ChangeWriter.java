// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb.v0_6.impl;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;


/**
 * Writes changes to a database.
 * 
 * @author Brett Henderson
 */
public class ChangeWriter {

	private DatabaseContext dbCtx;
	private ActionDao actionDao;
	private UserDao userDao;
	private ChangeDao changeDao;
	private Set<Integer> userSet;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @throws SQLException
	 */
	public ChangeWriter(DatabaseContext dbCtx) throws SQLException {
		this.dbCtx = dbCtx;

		actionDao = new ActionDao(dbCtx);
		userDao = new UserDao(dbCtx, actionDao);
		changeDao = new ChangeDao(dbCtx, actionDao);

		userSet = new HashSet<Integer>();
	}


	/**
	 * Performs any validation and pre-processing required for all entity types.
	 */
	private void processEntityPrerequisites(Entity entity) {
		// We can't write an entity with a null timestamp.
		if (entity.getTimestamp() == null) {
			throw new OsmosisRuntimeException("Entity(" + entity.getType() + ") " + entity.getId()
					+ " does not have a timestamp set.");
		}
	}


	/**
	 * Writes the specified node change to the database.
	 * 
	 * @param node
	 *            The node to be written.
	 * @param action
	 *            The change to be applied.
	 */
	public void write(Node node, ChangeAction action) {
		processEntityPrerequisites(node);

		try {
			changeDao.saveChange(node, action);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Writes the specified way change to the database.
	 * 
	 * @param way
	 *            The way to be written.
	 * @param action
	 *            The change to be applied.
	 */
	public void write(Way way, ChangeAction action) {
		processEntityPrerequisites(way);

		try {
			changeDao.saveChange(way, action);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Writes the specified relation change to the database.
	 * 
	 * @param relation
	 *            The relation to be written.
	 * @param action
	 *            The change to be applied.
	 */
	public void write(Relation relation, ChangeAction action) {
		processEntityPrerequisites(relation);

		try {
			changeDao.saveChange(relation, action);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * Performs post-change database updates.
	 */
	public void complete() {
	}


	/**
	 * Releases all resources.
	 */
	public void release() {
		// Nothing to do.
	}
}
