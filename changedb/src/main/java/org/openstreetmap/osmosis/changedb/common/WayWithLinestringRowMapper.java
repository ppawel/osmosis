package org.openstreetmap.osmosis.changedb.common;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.WayRowMapper;
import org.postgis.LineString;
import org.postgis.PGgeometry;

public class WayWithLinestringRowMapper extends WayRowMapper {

	@Override
	public Way mapRow(ResultSet rs, int rowNumber) throws SQLException {
		WayWithLinestring way;
		Array nodeIdArray;
		Long[] nodeIds;
		List<WayNode> wayNodes;

		way = new WayWithLinestring(mapCommonEntityData(rs));

		nodeIdArray = rs.getArray("nodes");

		if (nodeIdArray != null) {
			nodeIds = (Long[]) nodeIdArray.getArray();
			wayNodes = way.getWayNodes();
			for (long nodeId : nodeIds) {
				wayNodes.add(new WayNode(nodeId));
			}
		}

		way.setLineString((PGgeometry) rs.getObject("linestring"));

		return way;
	}
}
