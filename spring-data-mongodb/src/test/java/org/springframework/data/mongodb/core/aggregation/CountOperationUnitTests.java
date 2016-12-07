/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Unit tests for {@link CountOperation}.
 * 
 * @author Mark Paluch
 */
public class CountOperationUnitTests {

	/**
	 * @see DATAMONGO-1549
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsEmptyFieldName() {
		new CountOperation("");
	}

	/**
	 * @see DATAMONGO-1549
	 */
	@Test
	public void shouldRenderCorrectly() {

		CountOperation countOperation = new CountOperation("field");
		DBObject dbObject = countOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, is(JSON.parse("{$count : \"field\" }")));
	}

	/**
	 * @see DATAMONGO-1549
	 */
	@Test
	public void countExposesFields() {

		CountOperation countOperation = new CountOperation("field");

		assertThat(countOperation.getFields().exposesNoFields(), is(false));
		assertThat(countOperation.getFields().exposesSingleFieldOnly(), is(true));
		assertThat(countOperation.getFields().getField("field"), notNullValue());
	}
}
