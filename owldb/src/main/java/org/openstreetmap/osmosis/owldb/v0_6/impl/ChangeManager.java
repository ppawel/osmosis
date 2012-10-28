package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.hstore.PGHStore;
import org.openstreetmap.osmosis.owldb.common.DatabaseContext;
import org.openstreetmap.osmosis.owldb.common.PointBuilder;
import org.postgis.PGgeometry;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


public class ChangeManager {
	private static final Logger LOG = Logger.getLogger(ChangeManager.class.getName());

	private final static String INSERT_SQL = "INSERT INTO changes "
			+ "(user_id, version, changeset_id, tstamp, action, change_type, el_type, el_id, tags, new_tags, geom, new_geom) VALUES "
			+ "(?, ?, ?, ?, ?::action, ?::change_type, ?::element_type, ?, ?, ?, ?, ?)";

	private SimpleJdbcTemplate jdbcTemplate;
	private PreparedStatement changeInsertStatement;
	private NodeDao nodeDao;
	private WayDao wayDao;
	private RelationDao relationDao;
	private PointBuilder pointBuilder;


	/**
	 * Creates a new instance.
	 * 
	 * @param dbCtx
	 *            The database context to use for accessing the database.
	 * @param actionDao
	 *            The dao to use for adding action records to the database.
	 * @throws SQLException
	 */
	public ChangeManager(DatabaseContext dbCtx, ActionDao actionDao) throws SQLException {
		nodeDao = new NodeDao(dbCtx, actionDao) {

			@Override
			protected List<FeaturePopulator<Node>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		wayDao = new WayDao(dbCtx, actionDao) {

			@Override
			protected List<FeaturePopulator<Way>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		relationDao = new RelationDao(dbCtx, actionDao) {

			@Override
			protected List<FeaturePopulator<Relation>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		jdbcTemplate = dbCtx.getSimpleJdbcTemplate();
		changeInsertStatement = dbCtx.getJdbcTemplate().getDataSource().getConnection().prepareStatement(INSERT_SQL);
		pointBuilder = new PointBuilder();
	}


	protected void processCommon(ChangeAction action, Entity existingEntity, Entity newEntity) throws SQLException {
		changeInsertStatement.setInt(1, newEntity.getUser().getId());
		changeInsertStatement.setInt(2, newEntity.getVersion());
		changeInsertStatement.setLong(3, newEntity.getChangesetId());
		changeInsertStatement.setTimestamp(4, new Timestamp(newEntity.getTimestamp().getTime()));
		changeInsertStatement.setString(5, action.toString().toUpperCase());
		changeInsertStatement.setString(6, "CREATE");
		changeInsertStatement.setString(7, newEntity.getType().toString().substring(0, 1));
		changeInsertStatement.setLong(8, newEntity.getId());
		changeInsertStatement.setObject(9, null);
		changeInsertStatement.setObject(10, getTags(newEntity));
		changeInsertStatement.setObject(11, null);
		changeInsertStatement.setObject(12, null);
	}


	public void process(ChangeAction action, Node existingNode, Node newNode) throws SQLException {
		processCommon(action, existingNode, newNode);

		if (action != ChangeAction.Create && existingNode != null) {
			changeInsertStatement.setObject(9, getTags(existingNode));
			changeInsertStatement.setObject(12,
					new PGgeometry(pointBuilder.createPoint(existingNode.getLatitude(), existingNode.getLongitude())));
		}

		changeInsertStatement.setObject(11,
				new PGgeometry(pointBuilder.createPoint(newNode.getLatitude(), newNode.getLongitude())));

		changeInsertStatement.executeUpdate();
	}


	public void process(ChangeAction action, Way existingWay, Way newWay) throws SQLException {
		processCommon(action, existingWay, newWay);

		if (action != ChangeAction.Create && existingWay != null) {
			changeInsertStatement.setObject(9, getTags(existingWay));

			PGgeometry linestring = (PGgeometry) existingWay.getMetaTags().get("linestring");

			if (linestring != null) {
				if (linestring.getGeometry().numPoints() == 1) {
					// Yes - this happens in OSM data - lines with 1 point...
					// Need to convert them to POINT geometry.
					changeInsertStatement.setObject(11, new PGgeometry(linestring.getGeometry().getPoint(0)));
				} else {
					changeInsertStatement.setObject(11, linestring);
				}
			}
		}

		changeInsertStatement.executeUpdate();
	}


	public void process(ChangeAction action, Relation existingRelation, Relation newRelation) throws SQLException {

	}


	protected PGHStore getTags(Entity entity) {
		Map<String, String> tags = new HashMap<String, String>();
		for (Tag tag : entity.getTags()) {
			tags.put(tag.getKey(), tag.getValue());
		}
		return new PGHStore(tags);
	}
}
