// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.openstreetmap.osmosis.owldb.common.SchemaVersionValidator;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ActionChangeWriter;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ChangeWriter;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ChangesetManager;
import org.openstreetmap.osmosis.owldb.v0_6.impl.InvalidActionsMode;


/**
 * A change sink writing to database tables. This aims to be suitable for
 * running at regular intervals with database overhead proportional to changeset
 * size.
 * 
 * @author Brett Henderson
 */
public class PostgreSqlChangeWriter implements ChangeSink {

	private ChangesetManager changesetManager;
	private ChangeWriter changeWriter;
	private Map<ChangeAction, ActionChangeWriter> actionWriterMap;
	private DatabaseContext dbCtx;
	private SchemaVersionValidator schemaVersionValidator;
	private Set<Long> seenChangesetIds;
	private boolean initialized;


	/**
	 * Creates a new instance.
	 * 
	 * @param loginCredentials
	 *            Contains all information required to connect to the database.
	 * @param preferences
	 *            Contains preferences configuring database behaviour.
	 * @param invalidActionsMode 
	 * @throws SQLException
	 */
	public PostgreSqlChangeWriter(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences, InvalidActionsMode invalidActionsMode)
			throws SQLException {
		dbCtx = new DatabaseContext(loginCredentials);
		changesetManager = new ChangesetManager(dbCtx);
		changeWriter = new ChangeWriter(dbCtx, invalidActionsMode);
		actionWriterMap = new HashMap<ChangeAction, ActionChangeWriter>();
		actionWriterMap.put(ChangeAction.Create, new ActionChangeWriter(changeWriter, ChangeAction.Create));
		actionWriterMap.put(ChangeAction.Modify, new ActionChangeWriter(changeWriter, ChangeAction.Modify));
		actionWriterMap.put(ChangeAction.Delete, new ActionChangeWriter(changeWriter, ChangeAction.Delete));

		schemaVersionValidator = new SchemaVersionValidator(dbCtx.getSimpleJdbcTemplate(), preferences);
		seenChangesetIds = new TreeSet<Long>();

		initialized = false;
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
		ChangeAction action;

		initialize();

		// Verify that the schema version is supported.
		schemaVersionValidator.validateVersion(PostgreSqlVersionConstants.SCHEMA_VERSION);

		action = change.getAction();

		if (!actionWriterMap.containsKey(action)) {
			throw new OsmosisRuntimeException("The action " + action + " is unrecognized.");
		}

		changesetManager.addChangesetIfRequired(change.getEntityContainer().getEntity().getChangesetId(), change
				.getEntityContainer().getEntity().getUser(), change.getEntityContainer().getEntity().getTimestamp());

		seenChangesetIds.add(change.getEntityContainer().getEntity().getChangesetId());

		// Process the entity using the action writer appropriate for the change
		// action.
		change.getEntityContainer().process(actionWriterMap.get(action));
	}


	/**
	 * {@inheritDoc}
	 */
	public void complete() {
		initialize();

		changeWriter.complete();

		for (Long changesetId : seenChangesetIds) {
			try {
				changesetManager.updateChangeset(changesetId);
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		dbCtx.commitTransaction();
	}


	/**
	 * {@inheritDoc}
	 */
	public void release() {
		changeWriter.release();

		dbCtx.release();
	}
}
