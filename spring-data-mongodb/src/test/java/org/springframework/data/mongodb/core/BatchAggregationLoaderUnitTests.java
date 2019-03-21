/*
 * Copyright 2017-2019 the original author or authors.
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

import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import java.util.List;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.MongoTemplate.BatchAggregationLoader;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

/**
 * Unit tests for {@link BatchAggregationLoader}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class BatchAggregationLoaderUnitTests {

	static final TypedAggregation<Person> AGGREGATION = newAggregation(Person.class,
			project().and("firstName").as("name"));

	@Mock MongoTemplate template;
	@Mock DbRefResolver dbRefResolver;
	@Mock CommandResult aggregationResult;
	@Mock CommandResult getMoreResult;

	BatchAggregationLoader loader;

	BasicDBObject luke = new BasicDBObject("name", "luke");
	BasicDBObject han = new BasicDBObject("name", "han");
	DBObject cursorWithoutMore = new BasicDBObject("firstBatch", singletonList(luke));
	DBObject cursorWithMore = new BasicDBObject("id", 123).append("firstBatch",
			singletonList(luke));
	DBObject cursorWithNoMore = new BasicDBObject("id", 0).append("nextBatch",
			singletonList(han));

	@Before
	public void setUp() {
		MongoMappingContext context = new MongoMappingContext();
		loader = new BatchAggregationLoader(template, new QueryMapper(new MappingMongoConverter(dbRefResolver, context)),
				context, ReadPreference.primary(), 10);
	}

	@Test // DATAMONGO-1824
	public void shouldLoadWithoutCursor() {

		when(template.executeCommand(any(DBObject.class), any(ReadPreference.class))).thenReturn(aggregationResult);
		when(aggregationResult.get("result")).thenReturn(singletonList(luke));

		DBObject result = loader.aggregate("person", AGGREGATION, Aggregation.DEFAULT_CONTEXT);
		assertThat((List<Object>) result.get("result"), IsCollectionContaining.<Object> hasItem(luke));

		verify(template).executeCommand(any(DBObject.class), any(ReadPreference.class));
		verifyNoMoreInteractions(template);
	}

	@Test // DATAMONGO-1824
	public void shouldLoadJustOneBatchWhenAlreadyDoneWithFirst() {

		when(template.executeCommand(any(DBObject.class), any(ReadPreference.class))).thenReturn(aggregationResult);
		when(aggregationResult.containsField("cursor")).thenReturn(true);
		when(aggregationResult.get("cursor")).thenReturn(cursorWithoutMore);

		DBObject result = loader.aggregate("person", AGGREGATION, Aggregation.DEFAULT_CONTEXT);

		assertThat((List<Object>) result.get("result"),
				IsCollectionContaining.<Object> hasItem(luke));

		verify(template).executeCommand(any(DBObject.class), any(ReadPreference.class));
		verifyNoMoreInteractions(template);
	}

	@Test // DATAMONGO-1824
	public void shouldBatchLoadWhenRequired() {

		when(template.executeCommand(any(DBObject.class), any(ReadPreference.class))).thenReturn(aggregationResult)
				.thenReturn(getMoreResult);
		when(aggregationResult.containsField("cursor")).thenReturn(true);
		when(aggregationResult.get("cursor")).thenReturn(cursorWithMore);
		when(getMoreResult.containsField("cursor")).thenReturn(true);
		when(getMoreResult.get("cursor")).thenReturn(cursorWithNoMore);

		DBObject result = loader.aggregate("person", AGGREGATION, Aggregation.DEFAULT_CONTEXT);
		assertThat((List<Object>) result.get("result"),
				IsCollectionContaining.<Object> hasItems(luke, han));

		verify(template, times(2)).executeCommand(any(DBObject.class), any(ReadPreference.class));
		verifyNoMoreInteractions(template);
	}
}
