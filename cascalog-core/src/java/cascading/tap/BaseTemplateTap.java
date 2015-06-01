/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.tap;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BaseTemplateTap<Config, Output> extends
		SinkTap<Config, Output> {
	/** Field LOG */
	private static final Logger LOG = LoggerFactory
			.getLogger(BaseTemplateTap.class);
	/** Field OPEN_FILES_THRESHOLD_DEFAULT */
	protected static final int OPEN_TAPS_THRESHOLD_DEFAULT = 300;

	private class TemplateCollector extends TupleEntryCollector {
		private final FlowProcess<? extends Config> flowProcess;
		private final Config conf;
		private final Fields parentFields;
		private final Fields pathFields;

		public TemplateCollector(FlowProcess<? extends Config> flowProcess) {
			super(Fields.asDeclaration(getSinkFields()));
			this.flowProcess = flowProcess;
			this.conf = flowProcess.getConfigCopy();
			this.parentFields = parent.getSinkFields();
			this.pathFields = ((TemplateScheme) getScheme()).pathFields;
		}

		private TupleEntryCollector getCollector(String path) {
			TupleEntryCollector collector = collectors.get(path);

			if (collector != null)
				return collector;

			try {
				LOG.debug("creating collector for parent: {}, path: {}",
						parent.getFullIdentifier(conf), path);

				collector = createTupleEntrySchemeCollector(flowProcess,
						parent, path);

				flowProcess.increment(Counters.Paths_Opened, 1);
			} catch (IOException exception) {
				throw new TapException("unable to open template path: " + path,
						exception);
			}

			if (collectors.size() > openTapsThreshold)
				purgeCollectors();

			collectors.put(path, collector);

			if (LOG.isInfoEnabled() && collectors.size() % 100 == 0)
				LOG.info("caching {} open Taps", collectors.size());

			return collector;
		}

		private void purgeCollectors() {
			int numToClose = Math.max(1, (int) (openTapsThreshold * .10));

			if (LOG.isInfoEnabled())
				LOG.info("removing {} open Taps from cache of size {}",
						numToClose, collectors.size());

			Set<String> removeKeys = new HashSet<String>();
			Set<String> keys = collectors.keySet();

			for (String key : keys) {
				if (numToClose-- == 0)
					break;

				removeKeys.add(key);
			}

			for (String removeKey : removeKeys)
				closeCollector(collectors.remove(removeKey));

			flowProcess.increment(Counters.Path_Purges, 1);
		}

		@Override
		public void close() {
			super.close();

			try {
				for (TupleEntryCollector collector : collectors.values())
					closeCollector(collector);
			} finally {
				collectors.clear();
			}
		}

		private void closeCollector(TupleEntryCollector collector) {
			if (collector == null)
				return;

			try {
				collector.close();

				flowProcess.increment(Counters.Paths_Closed, 1);
			} catch (Exception exception) {
				// do nothing
			}
		}

		protected void collect(TupleEntry tupleEntry) throws IOException {
			if (pathFields != null) {
				Tuple pathValues = tupleEntry.selectTuple(pathFields);
				String path = pathValues.format(pathTemplate);

				getCollector(path).add(tupleEntry.selectTuple(parentFields));
			} else {
				String path = tupleEntry.getTuple().format(pathTemplate);

				getCollector(path).add(tupleEntry);
			}
		}
	}

	/** Field parent */
	protected Tap parent;
	/** Field pathTemplate */
	protected String pathTemplate;
	/** Field keepParentOnDelete */
	protected boolean keepParentOnDelete = false;
	/** Field openTapsThreshold */
	protected int openTapsThreshold = OPEN_TAPS_THRESHOLD_DEFAULT;
	/** Field collectors */
	private final Map<String, TupleEntryCollector> collectors = new LinkedHashMap<String, TupleEntryCollector>(
			1000, .75f, true);

	protected abstract TupleEntrySchemeCollector createTupleEntrySchemeCollector(
			FlowProcess<? extends Config> flowProcess, Tap parent, String path)
			throws IOException;

	/**
	 * Method getParent returns the parent Tap of this TemplateTap object.
	 *
	 * @return the parent (type Tap) of this TemplateTap object.
	 */
	public Tap getParent() {
		return parent;
	}

	/**
	 * Method getPathTemplate returns the pathTemplate
	 * {@link java.util.Formatter} format String of this TemplateTap object.
	 *
	 * @return the pathTemplate (type String) of this TemplateTap object.
	 */
	public String getPathTemplate() {
		return pathTemplate;
	}

	@Override
	public String getIdentifier() {
		return parent.getIdentifier();
	}

	/**
	 * Method getOpenTapsThreshold returns the openTapsThreshold of this
	 * TemplateTap object.
	 *
	 * @return the openTapsThreshold (type int) of this TemplateTap object.
	 */
	public int getOpenTapsThreshold() {
		return openTapsThreshold;
	}

	
	@Override
	public TupleEntryCollector openForWrite(FlowProcess<? extends Config> flowProcess, Output output) throws IOException {
		return new TemplateCollector(flowProcess);
	}

	/** @see cascading.tap.Tap#createResource(Object) */
	public boolean createResource(Config conf) throws IOException {
		return parent.createResource(conf);
	}

	/** @see cascading.tap.Tap#deleteResource(Object) */
	public boolean deleteResource(Config conf) throws IOException {
		return keepParentOnDelete || parent.deleteResource(conf);
	}

	@Override
	public boolean commitResource(Config conf) throws IOException {
		return parent.commitResource(conf);
	}

	@Override
	public boolean rollbackResource(Config conf) throws IOException {
		return parent.rollbackResource(conf);
	}

	/** @see cascading.tap.Tap#resourceExists(Object) */
	public boolean resourceExists(Config conf) throws IOException {
		return parent.resourceExists(conf);
	}

	/** @see cascading.tap.Tap#getModifiedTime(Object) */
	@Override
	public long getModifiedTime(Config conf) throws IOException {
		return parent.getModifiedTime(conf);
	}

	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;
		if (object == null || getClass() != object.getClass())
			return false;
		if (!super.equals(object))
			return false;

		BaseTemplateTap that = (BaseTemplateTap) object;

		if (parent != null ? !parent.equals(that.parent) : that.parent != null)
			return false;
		if (pathTemplate != null ? !pathTemplate.equals(that.pathTemplate)
				: that.pathTemplate != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (parent != null ? parent.hashCode() : 0);
		result = 31 * result
				+ (pathTemplate != null ? pathTemplate.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\""
				+ pathTemplate + "\"]";
	}

	public enum Counters {
		Paths_Opened, Paths_Closed, Path_Purges
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate,
			int openTapsThreshold) {
		this(new TemplateScheme(parent.getScheme()));
		this.parent = parent;
		this.pathTemplate = pathTemplate;
		this.openTapsThreshold = openTapsThreshold;
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate, SinkMode sinkMode) {
		super(new TemplateScheme(parent.getScheme()), sinkMode);
		this.parent = parent;
		this.pathTemplate = pathTemplate;
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate,
			SinkMode sinkMode, boolean keepParentOnDelete, int openTapsThreshold) {
		super(new TemplateScheme(parent.getScheme()), sinkMode);
		this.parent = parent;
		this.pathTemplate = pathTemplate;
		this.keepParentOnDelete = keepParentOnDelete;
		this.openTapsThreshold = openTapsThreshold;
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate,
			Fields pathFields, int openTapsThreshold) {
		super(new TemplateScheme(parent.getScheme(), pathFields));
		this.parent = parent;
		this.pathTemplate = pathTemplate;
		this.openTapsThreshold = openTapsThreshold;
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate,
			Fields pathFields, SinkMode sinkMode) {
		super(new TemplateScheme(parent.getScheme(), pathFields), sinkMode);
		this.parent = parent;
		this.pathTemplate = pathTemplate;
	}

	protected BaseTemplateTap(Tap parent, String pathTemplate,
			Fields pathFields, SinkMode sinkMode, boolean keepParentOnDelete,
			int openTapsThreshold) {
		super(new TemplateScheme(parent.getScheme(), pathFields), sinkMode);
		this.parent = parent;
		this.pathTemplate = pathTemplate;
		this.keepParentOnDelete = keepParentOnDelete;
		this.openTapsThreshold = openTapsThreshold;
	}

	protected BaseTemplateTap(Scheme<Config, ?, Output, ?, ?> scheme,
			SinkMode sinkMode) {
		super(scheme, sinkMode);
	}

	protected BaseTemplateTap(Scheme<Config, ?, Output, ?, ?> scheme) {
		super(scheme);
	}

	public static class TemplateScheme<Config, Output> extends
			Scheme<Config, Void, Output, Void, Void> {
		private final Scheme scheme;
		private final Fields pathFields;

		public TemplateScheme(Scheme scheme) {
			this.scheme = scheme;
			this.pathFields = null;
		}

		public TemplateScheme(Scheme scheme, Fields pathFields) {
			this.scheme = scheme;

			if (pathFields == null || pathFields.isAll())
				this.pathFields = null;
			else if (pathFields.isDefined())
				this.pathFields = pathFields;
			else
				throw new IllegalArgumentException(
						"pathFields must be defined or the ALL substitution, got: "
								+ pathFields.printVerbose());
		}

		public Fields getSinkFields() {
			if (pathFields == null || scheme.getSinkFields().isAll())
				return scheme.getSinkFields();

			return Fields.merge(scheme.getSinkFields(), pathFields);
		}

		public void setSinkFields(Fields sinkFields) {
			scheme.setSinkFields(sinkFields);
		}

		public Fields getSourceFields() {
			return scheme.getSourceFields();
		}

		public void setSourceFields(Fields sourceFields) {
			scheme.setSourceFields(sourceFields);
		}

		public int getNumSinkParts() {
			return scheme.getNumSinkParts();
		}

		public void setNumSinkParts(int numSinkParts) {
			scheme.setNumSinkParts(numSinkParts);
		}

		@Override
		public void sourceConfInit(FlowProcess<? extends Config> flowProcess,
				Tap<Config, Void, Output> tap, Config conf) {
			scheme.sourceConfInit(flowProcess, tap, conf);
		}

		@Override
		public void sourcePrepare(FlowProcess<? extends Config> flowProcess,
				SourceCall<Void, Void> sourceCall) throws IOException {
			scheme.sourcePrepare(flowProcess, sourceCall);
		}

		@Override
		public boolean source(FlowProcess<? extends Config> flowProcess,
				SourceCall<Void, Void> sourceCall) throws IOException {
			throw new UnsupportedOperationException("not supported");
		}

		@Override
		public void sourceCleanup(FlowProcess<? extends Config> flowProcess,
				SourceCall<Void, Void> sourceCall) throws IOException {
			scheme.sourceCleanup(flowProcess, sourceCall);
		}

		@Override
		public void sinkConfInit(FlowProcess<? extends Config> flowProcess,
				Tap<Config, Void, Output> tap, Config conf) {
			scheme.sinkConfInit(flowProcess, tap, conf);
		}

		@Override
		public void sinkPrepare(FlowProcess<? extends Config> flowProcess,
				SinkCall<Void, Output> sinkCall) throws IOException {
			scheme.sinkPrepare(flowProcess, sinkCall);
		}

		@Override
		public void sink(FlowProcess<? extends Config> flowProcess,
				SinkCall<Void, Output> sinkCall) throws IOException {
			throw new UnsupportedOperationException("should never be called");
		}

		@Override
		public void sinkCleanup(FlowProcess<? extends Config> flowProcess,
				SinkCall<Void, Output> sinkCall) throws IOException {
			scheme.sinkCleanup(flowProcess, sinkCall);
		}
	}
}
