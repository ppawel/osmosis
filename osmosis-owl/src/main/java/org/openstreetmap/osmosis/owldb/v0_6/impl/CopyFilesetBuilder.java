// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.owldb.v0_6.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openstreetmap.osmosis.core.container.v0_6.BoundContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.container.v0_6.EntityProcessor;
import org.openstreetmap.osmosis.core.container.v0_6.NodeContainer;
import org.openstreetmap.osmosis.core.container.v0_6.RelationContainer;
import org.openstreetmap.osmosis.core.container.v0_6.WayContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.OsmUser;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.lifecycle.CompletableContainer;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.hstore.PGHStore;
import org.openstreetmap.osmosis.owldb.common.CopyFileWriter;
import org.openstreetmap.osmosis.owldb.common.NodeLocationStoreType;
import org.openstreetmap.osmosis.owldb.common.PointBuilder;
import org.postgis.Geometry;


/**
 * An OSM data sink for storing all data to a set of database dump files. These
 * files can be used for populating an empty database.
 * 
 * @author Brett Henderson
 * @author Paweł Paprota
 */
public class CopyFilesetBuilder implements Sink, EntityProcessor {

	private WayGeometryBuilder wayGeometryBuilder;
	private CompletableContainer writerContainer;
	private MemberTypeValueMapper memberTypeValueMapper;
	private CopyFileWriter userWriter;
	private CopyFileWriter nodeWriter;
	private CopyFileWriter wayWriter;
	private CopyFileWriter relationWriter;
	private CopyFileWriter relationMemberWriter;
	private PointBuilder pointBuilder;
	private Set<Integer> userSet;


	/**
	 * Creates a new instance.
	 * 
	 * @param copyFileset
	 *            The set of COPY files to be populated.
	 * @param storeType
	 *            The node location storage type used by the geometry builders.
	 */
	public CopyFilesetBuilder(CopyFileset copyFileset, NodeLocationStoreType storeType) {
		writerContainer = new CompletableContainer();

		wayGeometryBuilder = new WayGeometryBuilder(storeType);

		userWriter = writerContainer.add(new CopyFileWriter(copyFileset.getUserFile()));
		nodeWriter = writerContainer.add(new CopyFileWriter(copyFileset.getNodeFile()));
		wayWriter = writerContainer.add(new CopyFileWriter(copyFileset.getWayFile()));
		relationWriter = writerContainer.add(new CopyFileWriter(copyFileset.getRelationFile()));
		relationMemberWriter = writerContainer.add(new CopyFileWriter(copyFileset.getRelationMemberFile()));

		pointBuilder = new PointBuilder();
		memberTypeValueMapper = new MemberTypeValueMapper();
		memberTypeValueMapper = new MemberTypeValueMapper();

		userSet = new HashSet<Integer>();
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
		OsmUser user;

		// Write a user entry if the user doesn't already exist.
		user = entityContainer.getEntity().getUser();
		if (!user.equals(OsmUser.NONE)) {
			if (!userSet.contains(user.getId())) {
				userWriter.writeField(user.getId());
				userWriter.writeField(user.getName());
				userWriter.endRecord();

				userSet.add(user.getId());
			}
		}

		// Process the entity itself.
		entityContainer.process(this);
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(BoundContainer boundContainer) {
		// Do nothing.
	}


	private PGHStore buildTags(Entity entity) {
		PGHStore tags;

		tags = new PGHStore();
		for (Tag tag : entity.getTags()) {
			tags.put(tag.getKey(), tag.getValue());
		}

		return tags;
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(NodeContainer nodeContainer) {
		Node node = nodeContainer.getEntity();

		nodeWriter.writeField(node.getId());
		nodeWriter.writeField(node.getVersion());
		nodeWriter.writeField(node.getRev());
		nodeWriter.writeField(node.isVisible());
		nodeWriter.writeField(node.isCurrent());
		nodeWriter.writeField(node.getUser().getId());
		nodeWriter.writeField(node.getTimestamp());
		nodeWriter.writeField(node.getChangesetId());
		nodeWriter.writeField(buildTags(node));

		if (node.isVisible()) {
			nodeWriter.writeField(pointBuilder.createPoint(node.getLatitude(), node.getLongitude()));
		} else {
			nodeWriter.writeField((Geometry) null);
		}

		nodeWriter.endRecord();

		// wayGeometryBuilder.addNodeLocation(node);
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(WayContainer wayContainer) {
		Way way = wayContainer.getEntity();
		List<Long> nodeIds;

		nodeIds = new ArrayList<Long>(way.getWayNodes().size());
		for (WayNode wayNode : way.getWayNodes()) {
			nodeIds.add(wayNode.getNodeId());
		}

		wayWriter.writeField(way.getId());
		wayWriter.writeField(way.getVersion());
		wayWriter.writeField(way.isVisible());
		wayWriter.writeField(way.isCurrent());
		wayWriter.writeField(way.getUser().getId());
		wayWriter.writeField(way.getTimestamp());
		wayWriter.writeField(way.getChangesetId());
		wayWriter.writeField(buildTags(way));
		wayWriter.writeField(nodeIds);
		// wayWriter.writeField((Geometry) null);
		// wayGeometryBuilder.createWayLinestring(way));
		wayWriter.endRecord();
	}


	/**
	 * {@inheritDoc}
	 */
	public void process(RelationContainer relationContainer) {
		Relation relation = relationContainer.getEntity();
		int memberSequenceId;

		relationWriter.writeField(relation.getId());
		relationWriter.writeField(relation.getVersion());
		relationWriter.writeField(relation.getRev());
		relationWriter.writeField(relation.isVisible());
		relationWriter.writeField(relation.isCurrent());
		relationWriter.writeField(relation.getUser().getId());
		relationWriter.writeField(relation.getTimestamp());
		relationWriter.writeField(relation.getChangesetId());
		relationWriter.writeField(buildTags(relation));
		relationWriter.endRecord();

		memberSequenceId = 0;
		for (RelationMember member : relation.getMembers()) {
			relationMemberWriter.writeField(relation.getId());
			relationMemberWriter.writeField(relation.getVersion());
			relationMemberWriter.writeField(member.getMemberId());
			relationMemberWriter.writeField(memberTypeValueMapper.getMemberType(member.getMemberType()));
			relationMemberWriter.writeField(member.getMemberRole());
			relationMemberWriter.writeField(memberSequenceId++);
			relationMemberWriter.endRecord();
		}
	}


	/**
	 * Writes any buffered data to the database and commits.
	 */
	public void complete() {
		writerContainer.complete();
	}


	/**
	 * Releases all resources.
	 */
	public void release() {
		writerContainer.release();
	}
}
