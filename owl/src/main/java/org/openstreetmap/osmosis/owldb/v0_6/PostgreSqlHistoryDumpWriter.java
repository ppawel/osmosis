// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.owldb.common.NodeLocationStoreType;


/**
 * Same as {@link PostgreSqlDumpWriter} but works with full history files and is
 * able to insert multiple versions of an entity into the database.
 * 
 * @author Paweł Paprota
 */
public class PostgreSqlHistoryDumpWriter extends PostgreSqlDumpWriter {
	private static final Logger LOG = Logger.getLogger(PostgreSqlHistoryDumpWriter.class.getName());

	// Number of last entity versions to store.
	// TODO: should be a task parameter?
	private static int NUM_VERSIONS = -1;

	private Long currentId;

	// Holds entity versions.
	private List<EntityContainer> lastVersions;


	/**
	 * Creates a new instance.
	 * 
	 * @param filePrefix
	 *            The prefix to prepend to all generated file names.
	 * @param storeType
	 *            The node location storage type used by the geometry builders.
	 */
	public PostgreSqlHistoryDumpWriter(File filePrefix, NodeLocationStoreType storeType) {
		super(filePrefix, storeType);
		lastVersions = new ArrayList<EntityContainer>();
	}


	/**
	 * {@inheritDoc}
	 */
	public void initialize(Map<String, Object> metaData) {
		// Do nothing.
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(EntityContainer entityContainer) {
		if (currentId != null && currentId != entityContainer.getEntity().getId()) {
			// Current entity is a new one so now we need to process the
			// previous one's history and reset.
			int rev = 1;
			lastVersions.get(lastVersions.size() - 1).getEntity().setCurrent(true);
			for (EntityContainer container : lastVersions) {
				container.getEntity().setRev(rev++);
				super.process(container);
			}
			lastVersions.clear();
		}

		lastVersions.add(entityContainer);
		if (NUM_VERSIONS != -1 && lastVersions.size() > NUM_VERSIONS) {
			lastVersions.remove(0);
		}

		currentId = entityContainer.getEntity().getId();
	}
}
