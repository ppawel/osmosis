// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb.v0_6.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.changedb.common.NoSuchRecordException;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.SqlParameter;


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
	 */
	public ChangeWriter(DatabaseContext dbCtx) {
		this.dbCtx = dbCtx;

		actionDao = new ActionDao(dbCtx);
		userDao = new UserDao(dbCtx, actionDao);
		changeDao = new ChangeDao(dbCtx, actionDao);

		userSet = new HashSet<Integer>();
	}


	/**
	 * Writes the specified user to the database.
	 * 
	 * @param user
	 *            The user to write.
	 */
	private void writeUser(OsmUser user) {
		// Entities without a user assigned should not be written.
		if (!OsmUser.NONE.equals(user)) {
			// Users will only be updated in the database once per changeset
			// run.
			if (!userSet.contains(user.getId())) {
				int userId;
				OsmUser existingUser;

				userId = user.getId();

				try {
					existingUser = userDao.getUser(userId);

					if (!user.equals(existingUser)) {
						userDao.updateUser(user);
					}

				} catch (NoSuchRecordException e) {
					userDao.addUser(user);
				}

				userSet.add(user.getId());
			}
		}
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

		// Process the user data.
		writeUser(entity.getUser());
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

		changeDao.saveChange(node, action);
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

		changeDao.saveChange(way, action);
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

		changeDao.saveChange(relation, action);
	}


	/**
	 * Performs post-change database updates.
	 */
	public void complete() {
		dbCtx.getJdbcTemplate().call(new CallableStatementCreator() {
			@Override
			public CallableStatement createCallableStatement(Connection con) throws SQLException {
				return con.prepareCall("{call osmosisUpdate()}");
			}
		}, new ArrayList<SqlParameter>());

		// Clear all action records.
		actionDao.truncate();
	}


	/**
	 * Releases all resources.
	 */
	public void release() {
		// Nothing to do.
	}
}
