// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlChangeWriterFactory;


/**
 * The plugin loader for the OWL Schema tasks.
 * 
 * @author Paweł Paprota
 */
public class OwlDbPluginLoader implements PluginLoader {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, TaskManagerFactory> loadTaskFactories() {
		Map<String, TaskManagerFactory> factoryMap;

		factoryMap = new HashMap<String, TaskManagerFactory>();

		factoryMap.put("write-owldb-change", new PostgreSqlChangeWriterFactory());
		//factoryMap.put("write-owldb-history", new PostgreSqlCopyWriterFactory());

		return factoryMap;
	}
}
