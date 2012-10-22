package org.openstreetmap.osmosis.changedb.v0_6.impl;

// This software is released into the Public Domain.  See copying.txt for details.

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
	private static final String SQL_UPDATE_WAY_BBOX = "UPDATE ways w SET bbox = ("
			+ " SELECT ST_Envelope(ST_Collect(n.geom))" + " FROM nodes n INNER JOIN way_nodes wn ON wn.node_id = n.id"
			+ " WHERE wn.way_id = w.id" + " )" + " WHERE w.id IN ("
			+ " SELECT w.id FROM ways w INNER JOIN way_nodes wn ON w.id = wn.way_id WHERE wn.node_id = ? GROUP BY w.id"
			+ " )";
	private static final String SQL_UPDATE_WAY_LINESTRING = "UPDATE ways w SET linestring = ("
			+ " SELECT ST_MakeLine(c.geom) AS way_line FROM ("
			+ " SELECT n.geom AS geom FROM nodes n INNER JOIN way_nodes wn ON n.id = wn.node_id"
			+ " WHERE (wn.way_id = w.id) ORDER BY wn.sequence_id" + " ) c" + " )" + " WHERE w.id IN ("
			+ " SELECT w.id FROM ways w INNER JOIN way_nodes wn ON w.id = wn.way_id WHERE wn.node_id = ? GROUP BY w.id"
			+ " )";

	private SimpleJdbcTemplate jdbcTemplate;
	private DatabaseCapabilityChecker capabilityChecker;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @param actionDao
	 *            The dao to use for adding action records to the database.
	 */
	public ChangeDao(DatabaseContext dbCtx, ActionDao actionDao) {
		jdbcTemplate = dbCtx.getSimpleJdbcTemplate();
		capabilityChecker = new DatabaseCapabilityChecker(dbCtx);
	}


	public void saveChange(Entity entity, ChangeAction action) {

	}

}
