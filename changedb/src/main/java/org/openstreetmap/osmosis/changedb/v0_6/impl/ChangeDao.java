package org.openstreetmap.osmosis.changedb.v0_6.impl;

// This software is released into the Public Domain.  See copying.txt for details.

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openstreetmap.osmosis.changedb.common.DatabaseContext;
import org.openstreetmap.osmosis.changedb.common.PointBuilder;
import org.openstreetmap.osmosis.changedb.common.WayWithLinestring;
import org.openstreetmap.osmosis.changedb.common.WayWithLinestringMapper;
import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.task.common.ChangeAction;
import org.openstreetmap.osmosis.hstore.PGHStore;
import org.openstreetmap.osmosis.pgsnapshot.common.NoSuchRecordException;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.ActionDao;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.EntityDao;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.NodeMapper;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.RelationMapper;
import org.postgis.LineString;
import org.postgis.PGgeometry;
import org.postgis.Point;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;


/**
 * Performs all change-specific db operations.
 * 
 * @author Paweł Paprota
 */
public class ChangeDao {
	private final static String INSERT_SQL = "INSERT INTO changes "
			+ "(user_id, version, changeset_id, tstamp, action, element_type, element_id, old_tags, new_tags, old_geom, new_geom) VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private SimpleJdbcTemplate jdbcTemplate;
	private PreparedStatement changeInsertStatement;
	private EntityDao<Node> nodeDao;
	private EntityDao<Way> wayDao;
	private EntityDao<Relation> relationDao;
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
	public ChangeDao(DatabaseContext dbCtx, ActionDao actionDao) throws SQLException {
		nodeDao = new EntityDao<Node>(dbCtx.getSimpleJdbcTemplate(), new NodeMapper(), actionDao) {

			@Override
			protected List<FeaturePopulator<Node>> getFeaturePopulators(String tablePrefix) {
				return Collections.emptyList();
			}
		};

		wayDao = new EntityDao<Way>(dbCtx.getSimpleJdbcTemplate(), new WayWithLinestringMapper(), actionDao) {

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
		pointBuilder = new PointBuilder();
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
		changeInsertStatement.setObject(11, null);

		try {
			if (entity.getType() == EntityType.Node) {
				process((Node) entity);
			} else if (entity.getType() == EntityType.Way) {
				process((Way) entity);
			} else if (entity.getType() == EntityType.Relation) {
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


	protected void process(Node node) throws SQLException {
		if (node.getVersion() > 1) {
			Node oldNode = nodeDao.getEntity(node.getId());
			changeInsertStatement.setObject(8, getTags(oldNode));
			changeInsertStatement.setObject(11,
					new PGgeometry(pointBuilder.createPoint(oldNode.getLatitude(), oldNode.getLongitude())));
		}

		changeInsertStatement.setObject(10,
				new PGgeometry(pointBuilder.createPoint(node.getLatitude(), node.getLongitude())));
	}


	protected void process(Way way) throws SQLException {
		if (way.getVersion() > 1) {
			WayWithLinestring oldWay = (WayWithLinestring) wayDao.getEntity(way.getId());
			changeInsertStatement.setObject(8, getTags(oldWay));

			if (oldWay.getLineString() != null) {
				if (oldWay.getLineString().getGeometry().numPoints() == 1) {
					LineString lineString = new LineString(new Point[] {
							oldWay.getLineString().getGeometry().getPoint(0),
							oldWay.getLineString().getGeometry().getPoint(0) });
					lineString.setSrid(4326);
					changeInsertStatement.setObject(10, new PGgeometry(lineString));
				} else {
					changeInsertStatement.setObject(10, oldWay.getLineString());
				}
			}
		}
	}
}
