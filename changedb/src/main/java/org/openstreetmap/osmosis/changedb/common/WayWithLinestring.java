package org.openstreetmap.osmosis.changedb.common;

import org.openstreetmap.osmosis.core.domain.v0_6.CommonEntityData;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.postgis.PGgeometry;


public class WayWithLinestring extends Way {
	private PGgeometry lineString;


	public WayWithLinestring(CommonEntityData data) {
		super(data);
	}


	public PGgeometry getLineString() {
		return lineString;
	}


	public void setLineString(PGgeometry lineString) {
		this.lineString = lineString;
	}
}
