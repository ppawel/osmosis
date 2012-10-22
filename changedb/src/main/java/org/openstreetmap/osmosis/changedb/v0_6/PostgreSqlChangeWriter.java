// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb.v0_6;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.changedb.common.SchemaVersionValidator;
import org.openstreetmap.osmosis.changedb.v0_6.impl.ChangeDbWriter;
import org.openstreetmap.osmosis.changedb.v0_6.impl.ChangeWriter;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;


/**
 * A change sink writing to database tables. This aims to be suitable for
 * running at regular intervals with database overhead proportional to changeset
 * size.
 * 
 * @author Brett Henderson
 */
public class PostgreSqlChangeWriter implements ChangeSink {

	private ChangeWriter changeWriter;
	private Map<ChangeAction, ChangeDbWriter> actionWriterMap;
	private DatabaseContext dbCtx;
	private SchemaVersionValidator schemaVersionValidator;
	private boolean initialized;


	/**
	 * Creates a new instance.
	 * 
	 * @param loginCredentials
	 *            Contains all information required to connect to the database.
	 * @param preferences
	 *            Contains preferences configuring database behaviour.
	 * @throws SQLException 
	 */
	public PostgreSqlChangeWriter(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences) throws SQLException {
		dbCtx = new DatabaseContext(loginCredentials);
		changeWriter = new ChangeWriter(dbCtx);
		actionWriterMap = new HashMap<ChangeAction, ChangeDbWriter>();
		actionWriterMap.put(ChangeAction.Create, new ChangeDbWriter(changeWriter, ChangeAction.Create));
		actionWriterMap.put(ChangeAction.Modify, new ChangeDbWriter(changeWriter, ChangeAction.Modify));
		actionWriterMap.put(ChangeAction.Delete, new ChangeDbWriter(changeWriter, ChangeAction.Delete));

		schemaVersionValidator = new SchemaVersionValidator(dbCtx.getSimpleJdbcTemplate(), preferences);

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
