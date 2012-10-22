// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.changedb;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.changedb.v0_6.PostgreSqlChangeWriterFactory;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;


/**
 * The plugin loader for the PostgreSQL ChangeDB Schema tasks.
 * 
 * @author Paweł Paprota
 * @author Brett Henderson
 */
public class ChangeDbPluginLoader implements PluginLoader {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, TaskManagerFactory> loadTaskFactories() {
		Map<String, TaskManagerFactory> factoryMap;

		factoryMap = new HashMap<String, TaskManagerFactory>();

		// factoryMap.put("write-pgsql", new PostgreSqlCopyWriterFactory());
		factoryMap.put("write-changedb-change", new PostgreSqlChangeWriterFactory());

		return factoryMap;
	}
}
