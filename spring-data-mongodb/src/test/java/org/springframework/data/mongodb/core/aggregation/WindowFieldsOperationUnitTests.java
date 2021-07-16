/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.WindowFieldsOperation.ComputedField;
import org.springframework.data.mongodb.core.aggregation.WindowFieldsOperation.Window;
import org.springframework.data.mongodb.core.aggregation.WindowFieldsOperation.WindowFieldsOperationBuilder;
import org.springframework.data.mongodb.core.aggregation.WindowFieldsOperation.WindowOutput;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class WindowFieldsOperationUnitTests {

	@Test
	void xxx() {

		WindowFieldsOperation windowFieldsOperation = new WindowFieldsOperationBuilder().partitionByField("state") //
				.sortBy(Sort.by(Direction.ASC, "date")) //
				.output(new WindowOutput(new ComputedField("cumulativeQuantityForState",
						AccumulatorOperators.valueOf("qty").sum(), Window.documents("unbounded", "current")))) //
				.build(); //

		Document document = windowFieldsOperation.toDocument(contextFor(CakeSale.class));
		System.out.println("document.toJson(): " + document.toJson());
		assertThat(document).isEqualTo(Document.parse("{ $setWindowFields: { partitionBy: \"$state\", sortBy: { orderDate: 1 }, output: { cumulativeQuantityForState: { $sum: \"$quantity\", window: { documents: [ \"unbounded\", \"current\" ] } } } } }" ));
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter));
	}

	static class CakeSale {

		String state;

		@Field("orderDate") Date date;

		@Field("quantity") Integer qty;

	}

}
