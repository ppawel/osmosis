package org.openstreetmap.osmosis.owldb.v0_6.impl;

/**
 * Enumerates modes for the "invalidActions" option of the --write-owldb-change
 * task.
 * 
 * @author ppawel
 * 
 */
public enum InvalidActionsMode {
	/**
	 * Default behavior - ignore invalid actions completely.
	 */
	IGNORE,

	/**
	 * Log when invalid actions occur but don't do anything.
	 */
	LOG,

	/**
	 * Break processing when an invalid action occurs.
	 */
	BREAK;
}
