/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions.DomainTypeMapping;
import org.springframework.data.mongodb.core.aggregation.CountOperation;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Utility methods to map {@link org.springframework.data.mongodb.core.aggregation.Aggregation} pipeline definitions and
 * create type-bound {@link AggregationOperationContext}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
class AggregationUtil {

	QueryMapper queryMapper;
	MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	Lazy<AggregationOperationContext> untypedMappingContext;

	AggregationUtil(QueryMapper queryMapper,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		this.queryMapper = queryMapper;
		this.mappingContext = mappingContext;
		this.untypedMappingContext = Lazy
				.of(() -> new RelaxedTypeBasedAggregationOperationContext(Object.class, mappingContext, queryMapper));
	}

	AggregationOperationContext createAggregationContext(Aggregation aggregation, @Nullable Class<?> inputType) {

		if (aggregation.getOptions().getDomainTypeMapping() == DomainTypeMapping.NONE) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		if (!(aggregation instanceof TypedAggregation)) {

			if(inputType == null) {
				return untypedMappingContext.get();
			}

			if (aggregation.getOptions().getDomainTypeMapping() == DomainTypeMapping.STRICT
					&& !aggregation.getPipeline().containsUnionWith()) {
				return new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper);
			}

			return new RelaxedTypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper);
		}

		inputType = ((TypedAggregation) aggregation).getInputType();
		if (aggregation.getOptions().getDomainTypeMapping() == DomainTypeMapping.STRICT
				&& !aggregation.getPipeline().containsUnionWith()) {
			return new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper);
		}

		return new RelaxedTypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper);
	}

	/**
	 * Extract and map the aggregation pipeline into a {@link List} of {@link Document}.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	List<Document> createPipeline(Aggregation aggregation, AggregationOperationContext context) {

		if (ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return aggregation.toPipeline(context);
		}

		return mapAggregationPipeline(aggregation.toPipeline(context));
	}

	/**
	 * Extract the command and map the aggregation pipeline.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	Document createCommand(String collection, Aggregation aggregation, AggregationOperationContext context) {

		Document command = aggregation.toDocument(collection, context);

		if (!ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return command;
		}

		command.put("pipeline", mapAggregationPipeline(command.get("pipeline", List.class)));

		return command;
	}

	/**
	 * Create a {@code $count} aggregation for {@link Query} and optionally a {@link Class entity class}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass can be {@literal null} if the {@link Query} object is empty.
	 * @return the {@link Aggregation} pipeline definition to run a {@code $count} aggregation.
	 */
	Aggregation createCountAggregation(Query query, @Nullable Class<?> entityClass) {

		List<AggregationOperation> pipeline = computeCountAggregationPipeline(query, entityClass);

		Aggregation aggregation = entityClass != null ? Aggregation.newAggregation(entityClass, pipeline)
				: Aggregation.newAggregation(pipeline);
		aggregation.withOptions(AggregationOptions.builder().collation(query.getCollation().orElse(null)).build());

		return aggregation;
	}

	private List<AggregationOperation> computeCountAggregationPipeline(Query query, @Nullable Class<?> entityType) {

		CountOperation count = Aggregation.count().as("totalEntityCount");
		if (query.getQueryObject().isEmpty()) {
			return Collections.singletonList(count);
		}

		Assert.notNull(entityType, "Entity type must not be null!");

		Document mappedQuery = queryMapper.getMappedObject(query.getQueryObject(),
				mappingContext.getPersistentEntity(entityType));

		CriteriaDefinition criteria = new CriteriaDefinition() {

			@Override
			public Document getCriteriaObject() {
				return mappedQuery;
			}

			@Nullable
			@Override
			public String getKey() {
				return null;
			}
		};

		return Arrays.asList(Aggregation.match(criteria), count);
	}

	private List<Document> mapAggregationPipeline(List<Document> pipeline) {

		return pipeline.stream().map(val -> queryMapper.getMappedObject(val, Optional.empty()))
				.collect(Collectors.toList());
	}
}
