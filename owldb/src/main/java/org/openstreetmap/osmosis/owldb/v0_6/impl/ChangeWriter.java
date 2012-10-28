// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.openstreetmap.osmosis.owldb.common.NoSuchRecordException;


/**
 * Writes changes to a database.
 * 
 * @author Brett Henderson
 */
public class ChangeWriter {
	private static final Logger LOG = Logger.getLogger(ChangeWriter.class.getName());

	private DatabaseContext dbCtx;
	private ChangeManager changeManager;
	private ActionDao actionDao;
	private UserDao userDao;
	private NodeDao nodeDao;
	private WayDao wayDao;
	private RelationDao relationDao;
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
		changeManager = new ChangeManager(dbCtx, actionDao);
		userDao = new UserDao(dbCtx, actionDao);
		nodeDao = new NodeDao(dbCtx, actionDao);
		wayDao = new WayDao(dbCtx, actionDao);
		relationDao = new RelationDao(dbCtx, actionDao);

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
	 * @throws SQLException
	 */
	public void write(Node node, ChangeAction action) throws SQLException {
		processEntityPrerequisites(node);

		Node existingNode = null;

		// if (!ChangeAction.Create.equals(action)) {
		existingNode = nodeDao.getEntity(node.getId());
		// }

		changeManager.process(action, existingNode, node);

		// LOG.info("Node " + node.getId() + ", version = " + node.getVersion()
		// + (existingNode != null ? ", existing version = " +
		// existingNode.getId() : ""));

		// If this is a create or modify, we must create or modify the records
		// in the database. Note that we don't use the input source to
		// distinguish between create and modify, we make this determination
		// based on our current data set.
		if (ChangeAction.Create.equals(action) || ChangeAction.Modify.equals(action)) {
			if (existingNode != null) {
				nodeDao.modifyEntity(node);
			} else {
				nodeDao.addEntity(node);
			}
		} else {
			// Remove the node from the database.
			nodeDao.removeEntity(node.getId());
		}
	}


	/**
	 * Writes the specified way change to the database.
	 * 
	 * @param way
	 *            The way to be written.
	 * @param action
	 *            The change to be applied.
	 * @throws SQLException
	 */
	public void write(Way way, ChangeAction action) throws SQLException {
		processEntityPrerequisites(way);

		Way existingWay = null;

		// if (!ChangeAction.Create.equals(action)) {
		existingWay = wayDao.getEntity(way.getId());
		// }

		// If this is a create or modify, we must create or modify the records
		// in the database. Note that we don't use the input source to
		// distinguish between create and modify, we make this determination
		// based on our current data set.
		if (ChangeAction.Create.equals(action) || ChangeAction.Modify.equals(action)) {
			if (existingWay != null) {
				wayDao.modifyEntity(way);
			} else {
				wayDao.addEntity(way);
			}

			way = wayDao.getEntity(way.getId());
		} else {
			// Remove the way from the database.
			wayDao.removeEntity(way.getId());
		}

		changeManager.process(action, existingWay, way);
	}


	/**
	 * Writes the specified relation change to the database.
	 * 
	 * @param relation
	 *            The relation to be written.
	 * @param action
	 *            The change to be applied.
	 * @throws SQLException
	 */
	public void write(Relation relation, ChangeAction action) throws SQLException {
		processEntityPrerequisites(relation);

		Relation existingRelation = null;

		// if (!ChangeAction.Create.equals(action)) {
		existingRelation = relationDao.getEntity(relation.getId());
		// }

		changeManager.process(action, existingRelation, relation);

		// If this is a create or modify, we must create or modify the records
		// in the database. Note that we don't use the input source to
		// distinguish between create and modify, we make this determination
		// based on our current data set.
		if (ChangeAction.Create.equals(action) || ChangeAction.Modify.equals(action)) {
			if (existingRelation != null) {
				relationDao.modifyEntity(relation);
			} else {
				relationDao.addEntity(relation);
			}

		} else {
			// Remove the relation from the database.
			relationDao.removeEntity(relation.getId());
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
