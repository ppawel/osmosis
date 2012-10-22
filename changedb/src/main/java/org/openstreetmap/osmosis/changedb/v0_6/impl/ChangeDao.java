package org.openstreetmap.osmosis.changedb.v0_6.impl;

// This software is released into the Public Domain.  See copying.txt for details.

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
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
		changeInsertStatement.setObject(9, null);
		changeInsertStatement.setObject(10, null);

		changeInsertStatement.executeUpdate();
	}
}
