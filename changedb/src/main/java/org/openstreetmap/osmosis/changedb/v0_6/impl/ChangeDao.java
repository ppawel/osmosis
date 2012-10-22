package org.openstreetmap.osmosis.changedb.v0_6.impl;

// This software is released into the Public Domain.  See copying.txt for details.

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.hstore.PGHStore;
import org.openstreetmap.osmosis.pgsnapshot.common.DatabaseContext;
import org.openstreetmap.osmosis.pgsnapshot.common.NoSuchRecordException;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.ActionDao;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.EntityDao;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.NodeDao;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.RelationMapper;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.WayMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Performs all change-specific db operations.
 * 
 * @author Paweł Paprota
 */
public class ChangeDao {
	private final static String INSERT_SQL = "INSERT INTO changes "
			+ "(user_id, version, changeset_id, tstamp, action, element_type, element_id, old_tags, new_tags, geom) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private SimpleJdbcTemplate jdbcTemplate;
	private PreparedStatement changeInsertStatement;
	private NodeDao nodeDao;
	private EntityDao<Way> wayDao;
	private EntityDao<Relation> relationDao;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @param actionDao
	 *            The dao to use for adding action records to the database.
	 * @throws SQLException
	 */
	public ChangeDao(DatabaseContext dbCtx, ActionDao actionDao) throws SQLException {
		nodeDao = new NodeDao(dbCtx, actionDao);

		wayDao = new EntityDao<Way>(dbCtx.getSimpleJdbcTemplate(), new WayMapper(), actionDao) {

			@Override
			protected List<FeaturePopulator<Way>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		relationDao = new EntityDao<Relation>(dbCtx.getSimpleJdbcTemplate(), new RelationMapper(), actionDao) {

			@Override
			protected List<FeaturePopulator<Relation>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		jdbcTemplate = dbCtx.getSimpleJdbcTemplate();
		changeInsertStatement = dbCtx.getJdbcTemplate().getDataSource().getConnection().prepareStatement(INSERT_SQL);
	}


	public void saveChange(Entity entity, ChangeAction action) throws SQLException {
		changeInsertStatement.setInt(1, entity.getUser().getId());
		changeInsertStatement.setInt(2, entity.getVersion());
		changeInsertStatement.setLong(3, entity.getChangesetId());
		changeInsertStatement.setTimestamp(4, new Timestamp(entity.getTimestamp().getTime()));
		changeInsertStatement.setString(5, action.toString());
		changeInsertStatement.setString(6, entity.getType().toString());
		changeInsertStatement.setLong(7, entity.getId());
		changeInsertStatement.setObject(8, null);
		changeInsertStatement.setObject(9, getTags(entity));
		changeInsertStatement.setObject(10, null);

		Entity oldEntity = null;

		try {
			if (entity.getType() == EntityType.Node) {
				oldEntity = nodeDao.getEntity(entity.getId());
			} else if (entity.getType() == EntityType.Way) {
				oldEntity = wayDao.getEntity(entity.getId());
			} else if (entity.getType() == EntityType.Relation) {
				oldEntity = relationDao.getEntity(entity.getId());
			}

			if (oldEntity != null) {
				changeInsertStatement.setObject(8, getTags(oldEntity));
			}
		} catch (NoSuchRecordException e) {
		}

		changeInsertStatement.executeUpdate();
	}


	protected PGHStore getTags(Entity entity) {
		Map<String, String> tags = new HashMap<String, String>();
		for (Tag tag : entity.getTags()) {
			tags.put(tag.getKey(), tag.getValue());
		}
		return new PGHStore(tags);
	}
}
