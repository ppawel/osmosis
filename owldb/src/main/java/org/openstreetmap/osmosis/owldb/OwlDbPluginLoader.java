// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb;

import java.util.HashMap;
import java.util.Map;

import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlChangeWriterFactory;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlCopyWriterFactory;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlDatasetReaderFactory;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlDumpWriterFactory;
import org.openstreetmap.osmosis.owldb.v0_6.PostgreSqlTruncatorFactory;


/**
 * The plugin loader for the OWL Schema tasks.
 * 
 * @author Brett Henderson
 */
public class OwlDbPluginLoader implements PluginLoader {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<String, TaskManagerFactory> loadTaskFactories() {
		Map<String, TaskManagerFactory> factoryMap;

		factoryMap = new HashMap<String, TaskManagerFactory>();

		// factoryMap.put("write-owldb", new PostgreSqlCopyWriterFactory());
		// factoryMap.put("wp", new PostgreSqlCopyWriterFactory());
		// factoryMap.put("truncate-owldb", new PostgreSqlTruncatorFactory());
		// factoryMap.put("tp", new PostgreSqlTruncatorFactory());
		// factoryMap.put("write-owldb-dump", new
		// PostgreSqlDumpWriterFactory());
		// factoryMap.put("wpd", new PostgreSqlDumpWriterFactory());
		// factoryMap.put("read-owldb", new PostgreSqlDatasetReaderFactory());
		// factoryMap.put("rp", new PostgreSqlDatasetReaderFactory());
		factoryMap.put("write-owldb-change", new PostgreSqlChangeWriterFactory());
		factoryMap.put("woc", new PostgreSqlChangeWriterFactory());

		factoryMap.put("write-owldb-0.6", new PostgreSqlCopyWriterFactory());
		factoryMap.put("truncate-owldb-0.6", new PostgreSqlTruncatorFactory());
		factoryMap.put("write-owldb-dump-0.6", new PostgreSqlDumpWriterFactory());
		factoryMap.put("read-owldb-0.6", new PostgreSqlDatasetReaderFactory());
		factoryMap.put("write-owldb-change-0.6", new PostgreSqlChangeWriterFactory());

		return factoryMap;
	}
}
