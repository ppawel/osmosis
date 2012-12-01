package org.openstreetmap.osmosis.owldb.v0_6;

import java.sql.SQLException;
import java.util.Map;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ChangeWriter;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ChangesetManager;
import org.openstreetmap.osmosis.owldb.v0_6.impl.InvalidActionsMode;


public class PostgreSqlHistoryWriter implements Sink {

	private ChangesetManager changesetManager;
	private ChangeWriter changeWriter;
	private DatabaseContext dbCtx;


	public PostgreSqlHistoryWriter(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences,
			InvalidActionsMode invalidActionsMode) throws SQLException {
		dbCtx = new DatabaseContext(loginCredentials);
		changesetManager = new ChangesetManager(dbCtx);
		changeWriter = new ChangeWriter(dbCtx, invalidActionsMode);
	}


	@Override
	public void initialize(Map<String, Object> metaData) {
		// TODO Auto-generated method stub

	}


	@Override
	public void complete() {
		// TODO Auto-generated method stub

	}


	@Override
	public void release() {
		// TODO Auto-generated method stub

	}


	@Override
	public void process(EntityContainer entityContainer) {
		// TODO Auto-generated method stub

	}

}
