package org.openstreetmap.osmosis.owldb.v0_6;

import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.owldb.v0_6.impl.ActionDao;
import org.openstreetmap.osmosis.owldb.v0_6.impl.InvalidActionsMode;
import org.openstreetmap.osmosis.owldb.v0_6.impl.NodeDao;


public class PostgreSqlHistoryWriter extends PostgreSqlChangeWriter implements Sink {
	private static final Logger LOG = Logger.getLogger(PostgreSqlHistoryWriter.class.getName());

	private ActionDao actionDao;

	private NodeDao nodeDao;

	private Entity existingEntity;

	private Entity previousEntity;

	private EntityContainer previousEntityContainer;

	private int count;


	public PostgreSqlHistoryWriter(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences,
			InvalidActionsMode invalidActionsMode) throws SQLException {
		super(loginCredentials, preferences, invalidActionsMode);
		actionDao = new ActionDao(dbCtx);
		nodeDao = new NodeDao(dbCtx, actionDao);
	}


	@Override
	public void initialize(Map<String, Object> metaData) {
	}


	@Override
	public void complete() {
	}


	@Override
	public void release() {
	}


	@Override
	public void process(EntityContainer entityContainer) {
		Entity currentEntity = entityContainer.getEntity();

		if (previousEntity == null || (previousEntity.isVisible() && currentEntity.getId() != previousEntity.getId())) {
			if (existingEntity != null
					&& previousEntity != null
					&& ((previousEntity.getVersion() == existingEntity.getVersion() + 1) || (previousEntity
							.getVersion() == 1 && previousEntity.isVisible()))) {
				// LOG.info("Processing " + previousEntity);

				process(new ChangeContainer(previousEntityContainer, ChangeAction.Create), existingEntity);
				count++;

				if (count % 1000 == 0) {
					LOG.info("Committing... count = " + count);
					super.complete();
				}
			}

			existingEntity = nodeDao.getEntity(currentEntity.getId());
			// LOG.info(previousEntity.getId() + " " +
			// previousEntity.getVersion() + " (existing " + existingEntity +
			// ") "
			// + previousEntity.isVisible());
		}

		previousEntity = currentEntity;
		previousEntityContainer = entityContainer;
	}
}
