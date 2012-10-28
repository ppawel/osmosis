package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
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
import org.springframework.util.StringUtils;


public class ChangeManager {
	private static final Logger LOG = Logger.getLogger(ChangeManager.class.getName());

	private final static String[] INSERT_COLUMNS = new String[] { "el_type", "el_id", "version", "changeset_id",
			"tstamp", "action", "changed_tags", "changed_geom", "changed_members", "current_tags", "new_tags",
			"current_geom", "new_geom" };

	private final static String INSERT_SQL = "INSERT INTO changes " + "("
			+ StringUtils.arrayToCommaDelimitedString(INSERT_COLUMNS) + ") VALUES "
			+ "(?::element_type, ?, ?, ?, ?, ?::action, ?, ?, ?, ?, ?, ?, ?)";

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
		changeInsertStatement.setString(getParamIndex("el_type"), newEntity.getType().toString().substring(0, 1));
		changeInsertStatement.setLong(getParamIndex("el_id"), newEntity.getId());
		changeInsertStatement.setInt(getParamIndex("version"), newEntity.getVersion());
		changeInsertStatement.setLong(getParamIndex("changeset_id"), newEntity.getChangesetId());
		changeInsertStatement.setTimestamp(getParamIndex("tstamp"), new Timestamp(newEntity.getTimestamp().getTime()));
		changeInsertStatement.setString(getParamIndex("action"), action.toString().toUpperCase());
		changeInsertStatement.setBoolean(getParamIndex("changed_tags"), false);
		changeInsertStatement.setBoolean(getParamIndex("changed_geom"), false);
		changeInsertStatement.setBoolean(getParamIndex("changed_members"), false);
		changeInsertStatement.setObject(getParamIndex("current_geom"), null);
		changeInsertStatement.setObject(getParamIndex("current_tags"), null);
		changeInsertStatement.setObject(getParamIndex("new_geom"), null);
		changeInsertStatement.setObject(getParamIndex("new_tags"), null);

		switch (action) {
		case Create:
			changeInsertStatement.setObject(getParamIndex("new_geom"), getGeom(newEntity));
			changeInsertStatement.setObject(getParamIndex("new_tags"), new PGHStore(getTags(newEntity)));
			changeInsertStatement.setBoolean(getParamIndex("changed_tags"), true);
			changeInsertStatement.setBoolean(getParamIndex("changed_geom"), true);
			changeInsertStatement.setBoolean(getParamIndex("changed_members"), newEntity.getType() != EntityType.Node);
			break;

		case Modify:
			Map<String, String> currentTags = getTags(existingEntity);
			Map<String, String> newTags = getTags(newEntity);

			if (existingEntity != null) {
				changeInsertStatement.setObject(getParamIndex("current_geom"), getGeom(existingEntity));
				changeInsertStatement.setObject(getParamIndex("current_tags"), new PGHStore(currentTags));
			}

			changeInsertStatement.setObject(getParamIndex("new_geom"), getGeom(newEntity));
			changeInsertStatement.setObject(getParamIndex("new_tags"), new PGHStore(newTags));

			changeInsertStatement.setBoolean(getParamIndex("changed_tags"), !newTags.equals(currentTags));
			changeInsertStatement.setBoolean(getParamIndex("changed_geom"), true);
			changeInsertStatement.setBoolean(getParamIndex("changed_members"), newEntity.getType() != EntityType.Node);

			break;

		case Delete:
			if (existingEntity != null) {
				changeInsertStatement.setObject(getParamIndex("current_geom"), getGeom(existingEntity));
				changeInsertStatement.setObject(getParamIndex("current_tags"), new PGHStore(getTags(existingEntity)));
			}

			changeInsertStatement.setBoolean(getParamIndex("changed_tags"), true);
			changeInsertStatement.setBoolean(getParamIndex("changed_geom"), true);

			break;
		}
	}


	public void process(ChangeAction action, Node existingNode, Node newNode) throws SQLException {
		changeInsertStatement.clearParameters();
		processCommon(action, existingNode, newNode);
		changeInsertStatement.executeUpdate();
	}


	public void process(ChangeAction action, Way existingWay, Way newWay) throws SQLException {
		changeInsertStatement.clearParameters();
		processCommon(action, existingWay, newWay);
		changeInsertStatement.executeUpdate();
	}


	public void process(ChangeAction action, Relation existingRelation, Relation newRelation) throws SQLException {
		// TODO
	}


	protected PGgeometry getGeom(Entity entity) {
		PGgeometry result = null;

		if (entity.getType() == EntityType.Node) {
			Node node = (Node) entity;
			result = new PGgeometry(pointBuilder.createPoint(node.getLatitude(), node.getLongitude()));
		} else if (entity.getType() == EntityType.Way) {
			Way way = (Way) entity;

			result = (PGgeometry) way.getMetaTags().get("linestring");

			if (result != null && result.getGeometry().numPoints() == 1) {
				// Yes - this happens in OSM data - lines with 1 point...
				// Need to convert them to POINT geometry.
				result = new PGgeometry(result.getGeometry().getPoint(0));
			}
		}

		return result;
	}


	protected int getParamIndex(String column) {
		int index = Arrays.asList(INSERT_COLUMNS).indexOf(column) + 1;

		if (index <= 0) {
			throw new RuntimeException("param index is " + index + " for column " + column);
		}

		return index;
	}


	protected Map<String, String> getTags(Entity entity) {
		Map<String, String> tags = new HashMap<String, String>();

		if (entity != null) {
			for (Tag tag : entity.getTags()) {
				tags.put(tag.getKey(), tag.getValue());
			}
		}

		return tags;
	}
}
