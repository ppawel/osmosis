// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import org.openstreetmap.osmosis.core.database.DbFeature;
import org.openstreetmap.osmosis.core.database.FeatureCollectionLoader;
import org.openstreetmap.osmosis.core.database.FeaturePopulator;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;
import org.openstreetmap.osmosis.core.store.PeekableIterator;
import org.openstreetmap.osmosis.core.store.Storeable;


/**
 * Populates entities with their features using a sorted data source.
 * 
 * @param <Te>
 *            The type of entity to be populated.
 * @param <Tf>
 *            The type of feature to be added.
 * @param <Tdbf>
 *            The database feature class type. This is extensible to allow other attributes to be
 *            added to features such as a sequence number.
 */
public class FeaturePopulatorImpl<Te extends Entity, Tf extends Storeable, Tdbf extends DbFeature<Tf>> implements
		FeaturePopulator<Te> {
	
	private PeekableIterator<Tdbf> source;
	private FeatureCollectionLoader<Te, Tf> featureLoader;
	
	
	/**
	 * Creates a new instance.
	 * 
	 * @param source
	 *            The feature source.
	 * @param featureLoader
	 *            Provides access to the feature collection within the entity.
	 */
	public FeaturePopulatorImpl(ReleasableIterator<Tdbf> source,
			FeatureCollectionLoader<Te, Tf> featureLoader) {
		this.source = new PeekableIterator<Tdbf>(source);
		this.featureLoader = featureLoader;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void populateFeatures(Te entity) {
		// Add all applicable tags to the entity.
		while (source.hasNext()
				&& source.peekNext().getEntityId() == entity.getId()) {
			featureLoader.getFeatureCollection(entity).add(source.next().getFeature());
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void release() {
		source.release();
	}
}
