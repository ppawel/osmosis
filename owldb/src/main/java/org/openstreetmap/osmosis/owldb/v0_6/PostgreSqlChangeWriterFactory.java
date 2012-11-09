// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6;

import java.sql.SQLException;

import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.database.DatabaseTaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.v0_6.ChangeSinkManager;
import org.openstreetmap.osmosis.owldb.v0_6.impl.InvalidActionsMode;


/**
 * The task manager factory for a database change writer.
 * 
 * @author Brett Henderson
 */
public class PostgreSqlChangeWriterFactory extends DatabaseTaskManagerFactory {

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
		DatabaseLoginCredentials loginCredentials;
		DatabasePreferences preferences;

		// Get the task arguments.
		loginCredentials = getDatabaseLoginCredentials(taskConfig);
		preferences = getDatabasePreferences(taskConfig);

		try {
			return new ChangeSinkManager(taskConfig.getId(), new PostgreSqlChangeWriter(loginCredentials, preferences,
					getInvalidActionsMode(taskConfig)), taskConfig.getPipeArgs());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}


	/**
	 * Retrieves the InvalidActionsMode setting from the configuration.
	 * 
	 * @param configuration
	 */
	protected InvalidActionsMode getInvalidActionsMode(TaskConfiguration configuration) {
		InvalidActionsMode mode = InvalidActionsMode.IGNORE;
		String arg = configuration.getConfigArgs().get("invalidActionsMode");

		if (arg == null) {
			return mode;
		}

		if (arg.equalsIgnoreCase("log")) {
			mode = InvalidActionsMode.LOG;
		} else if (arg.equalsIgnoreCase("break")) {
			mode = InvalidActionsMode.BREAK;
		}

		return mode;
	}
}
