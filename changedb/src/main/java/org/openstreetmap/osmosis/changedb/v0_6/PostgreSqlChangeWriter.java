// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb.v0_6;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.changedb.v0_6.impl.ChangeDao;
import org.openstreetmap.osmosis.changedb.v0_6.impl.ChangesetManager;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;


/**
 * A change sink writing to database tables. This aims to be suitable for
 * running at regular intervals with database overhead proportional to changeset
 * size.
 * 
 * @author Brett Henderson
 */
public class PostgreSqlChangeWriter implements ChangeSink {

	private DatabaseContext dbCtx;
	private ChangesetManager changesetManager;
	private ChangeDao changeDao;
	private boolean initialized;
	private Set<Long> seenChangesetIds;


	/**
	 * Creates a new instance.
	 * 
	 * @param loginCredentials
	 *            Contains all information required to connect to the database.
	 * @param preferences
	 *            Contains preferences configuring database behaviour.
	 * @throws SQLException
	 */
	public PostgreSqlChangeWriter(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences)
			throws SQLException {
		dbCtx = new DatabaseContext(loginCredentials);
		changesetManager = new ChangesetManager(dbCtx);
		changeDao = new ChangeDao(dbCtx, null);
		initialized = false;
		seenChangesetIds = new TreeSet<Long>();
	}


	private void initialize() {
		if (!initialized) {
			dbCtx.beginTransaction();

			initialized = true;
		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void initialize(Map<String, Object> metaData) {
		// Do nothing.
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(ChangeContainer change) {
		initialize();

		long changesetId = change.getEntityContainer().getEntity().getChangesetId();

		seenChangesetIds.add(changesetId);
		changesetManager.addChangesetIfRequired(changesetId, change.getEntityContainer().getEntity().getUser());

		try {
			changeDao.saveChange(change.getEntityContainer().getEntity(), change.getAction());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void complete() {
		initialize();

		dbCtx.commitTransaction();

		for (Long changesetId : seenChangesetIds) {
			try {
				changesetManager.updateChangesetGeometry(changesetId);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


	/**
	 * {@inheritDoc}
	 */
	public void release() {
		dbCtx.release();
	}
}
