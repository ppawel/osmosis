package org.openstreetmap.osmosis.changedb.common;

import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.pgsnapshot.v0_6.impl.WayMapper;
import org.springframework.jdbc.core.RowMapper;


public class WayWithLinestringMapper extends WayMapper {

	public WayWithLinestringMapper() {
		super(false, true);
	}


	@Override
	public RowMapper<Way> getRowMapper() {
		return new WayWithLinestringRowMapper();
	}
}
