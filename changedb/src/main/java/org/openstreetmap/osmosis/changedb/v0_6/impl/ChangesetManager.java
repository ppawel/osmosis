// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb.v0_6.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.database.ReleasableStatementContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.lifecycle.Releasable;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableContainer;


/**
 * Creates and maintains changesets for a database conversation. This will
 * create a separate changeset for each user id making changes.
 * 
 * @author Brett Henderson
 */
public class ChangesetManager implements Releasable {

	private static final Logger LOG = Logger.getLogger(ChangesetManager.class.getName());

	private static final int MAX_CHANGESET_ID_CACHE_SIZE = 32768;

	private static final String SQL_INSERT_CHANGESET = "INSERT INTO changesets"
			+ " (id, user_id, created_at, closed_at, num_changes)" + " VALUES" + " (?, ?, NOW(), NOW(), 0)";

	private static final String SQL_UPDATE_CHANGESET_GEOM = "SELECT Osmosis_ChangeDb_UpdateChangesetGeom(?)";

	private static final String SQL_SELECT_CHANGESET_COUNT = "SELECT Count(*) AS changesetCount FROM changesets WHERE id = ?";

	private final DatabaseContext dbCtx;
	private final ReleasableContainer releasableContainer;
	private final ReleasableStatementContainer statementContainer;
	private PreparedStatement insertStatement;
	private PreparedStatement selectCountStatement;
	private PreparedStatement updateChangesetStatement;
	private Set<Long> knownChangesetIds;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for all database access.
	 */
	public ChangesetManager(DatabaseContext dbCtx) {
		this.dbCtx = dbCtx;

		releasableContainer = new ReleasableContainer();
		statementContainer = new ReleasableStatementContainer();

		releasableContainer.add(statementContainer);

		knownChangesetIds = new LinkedHashSet<Long>(32768);

		updateChangesetStatement = statementContainer
				.add(dbCtx.prepareStatementForStreaming(SQL_UPDATE_CHANGESET_GEOM));
	}


	private int readChangesetCount(ResultSet countSet) {
		ResultSet resultSet = countSet;

		try {
			int changesetCount;

			resultSet.next();
			changesetCount = resultSet.getInt("changesetCount");
			resultSet.close();
			resultSet = null;

			return changesetCount;

		} catch (SQLException e) {
			throw new OsmosisRuntimeException("Unable to read the changeset count.", e);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					LOG.log(Level.WARNING, "Unable to close result set.", e);
				}
			}
		}
	}


	private boolean doesChangesetExist(long changesetId) {
		if (knownChangesetIds.contains(changesetId)) {
			return true;
		}

		if (selectCountStatement == null) {
			selectCountStatement = statementContainer.add(dbCtx
					.prepareStatementForStreaming(SQL_SELECT_CHANGESET_COUNT));
		}

		try {
			int prmIndex;
			boolean changesetExists;

			// Check if the changeset exists.
			prmIndex = 1;
			selectCountStatement.setLong(prmIndex++, changesetId);

			changesetExists = readChangesetCount(selectCountStatement.executeQuery()) > 0;

			return changesetExists;

		} catch (SQLException e) {
			throw new OsmosisRuntimeException("Unable to check if a changeset " + changesetId + " exists.", e);
		}
	}


	private void addChangeset(long changesetId, long userId) {
		if (insertStatement == null) {
			insertStatement = statementContainer.add(dbCtx.prepareStatement(SQL_INSERT_CHANGESET));
		}

		try {
			int prmIndex;

			// Insert the new changeset record.
			prmIndex = 1;
			insertStatement.setLong(prmIndex++, changesetId);
			insertStatement.setLong(prmIndex++, userId);
			insertStatement.executeUpdate();

			// Add the changeset to the cache, and trim the cache if required.
			knownChangesetIds.add(changesetId);
			if (knownChangesetIds.size() > MAX_CHANGESET_ID_CACHE_SIZE) {
				Iterator<Long> i = knownChangesetIds.iterator();

				i.next();
				i.remove();
			}

		} catch (SQLException e) {
			throw new OsmosisRuntimeException("Unable to insert a new changeset for user with id " + userId + ".", e);
		}
	}


	/**
	 * Checks to see if the changeset already exists and adds it if it doesn't.
	 * 
	 * @param changesetId
	 *            The changeset identifier.
	 * @param user
	 *            The user who created the changeset.
	 */
	public void addChangesetIfRequired(long changesetId, OsmUser user) {
		if (!doesChangesetExist(changesetId)) {
			addChangeset(changesetId, user.getId());
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release() {
		releasableContainer.release();
	}


	public void updateChangesetGeometry(long changesetId) throws SQLException {
		LOG.info("Updating changeset " + changesetId + " geometry");
		updateChangesetStatement.setLong(1, changesetId);
		updateChangesetStatement.execute();
	}
}
