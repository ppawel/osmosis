// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6;

import java.io.File;
import java.util.Map;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.owldb.common.NodeLocationStoreType;
import org.openstreetmap.osmosis.owldb.v0_6.impl.CopyFilesetBuilder;
import org.openstreetmap.osmosis.owldb.v0_6.impl.DirectoryCopyFileset;


/**
 * An OSM data sink for storing all data to database dump files. This task is
 * intended for populating an empty database.
 * 
 * @author Brett Henderson
 */
public class PostgreSqlDumpWriter implements Sink {

	private CopyFilesetBuilder copyFilesetBuilder;


	/**
	 * Creates a new instance.
	 * 
	 * @param filePrefix
	 *            The prefix to prepend to all generated file names.
	 * 
	 * @param storeType
	 *            The node location storage type used by the geometry builders.
	 */
	public PostgreSqlDumpWriter(File filePrefix, NodeLocationStoreType storeType) {
		DirectoryCopyFileset copyFileset;

		copyFileset = new DirectoryCopyFileset(filePrefix);

		copyFilesetBuilder = new CopyFilesetBuilder(copyFileset, storeType);
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
		copyFilesetBuilder.process(entityContainer);
	}


	/**
	 * Writes any buffered data to the database and commits.
	 */
	public void complete() {
		copyFilesetBuilder.complete();
	}


	/**
	 * Releases all database resources.
	 */
	public void release() {
		copyFilesetBuilder.release();
	}
}
