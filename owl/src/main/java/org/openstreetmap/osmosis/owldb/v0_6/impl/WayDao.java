// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.util.Collections;
import java.util.List;

import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Performs all way-specific db operations.
 * 
 * @author Brett Henderson
 */
public class WayDao extends EntityDao<Way> {

	private static final String SQL_UPDATE_WAY_LINESTRING = "UPDATE ways w SET linestring = ("
			+ " SELECT ST_MakeLine(c.geom) AS way_line FROM ("
			+ " SELECT n.geom AS geom FROM nodes n INNER JOIN way_nodes wn ON n.id = wn.node_id"
			+ " WHERE (wn.way_id = w.id) ORDER BY wn.sequence_id" + " ) c" + " )" + " WHERE w.id  = ?";

	private SimpleJdbcTemplate jdbcTemplate;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @param actionDao
	 *            The dao to use for adding action records to the database.
	 */
	public WayDao(DatabaseContext dbCtx, ActionDao actionDao) {
		super(dbCtx.getSimpleJdbcTemplate(), new WayMapper(), actionDao);

		jdbcTemplate = dbCtx.getSimpleJdbcTemplate();
	}


	/**
	 * Updates the bounding box column for the specified way.
	 * 
	 * @param wayId
	 *            The way bounding box.
	 */
	private void updateWayGeometries(long wayId) {
		jdbcTemplate.update(SQL_UPDATE_WAY_LINESTRING, wayId);
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void addEntity(Way entity) {
		super.addEntity(entity);
		updateWayGeometries(entity.getId());
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void modifyEntity(Way entity) {
		super.modifyEntity(entity);
		updateWayGeometries(entity.getId());
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected List<FeaturePopulator<Way>> getFeaturePopulators(String tablePrefix) {
		return Collections.emptyList();
	}
}
