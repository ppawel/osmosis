// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.util.Collections;
import java.util.List;

import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Performs all node-specific db operations.
 * 
 * @author Brett Henderson
 */
public class NodeDao extends EntityDao<Node> {
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


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @param actionDao
	 *            The dao to use for adding action records to the database.
	 */
	public NodeDao(DatabaseContext dbCtx, ActionDao actionDao) {
		super(dbCtx.getSimpleJdbcTemplate(), new NodeMapper(), actionDao);

		jdbcTemplate = dbCtx.getSimpleJdbcTemplate();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected List<FeaturePopulator<Node>> getFeaturePopulators(String tablePrefix) {
		return Collections.emptyList();
	}
}
