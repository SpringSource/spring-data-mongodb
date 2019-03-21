/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.ConditionalOperator.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Unit tests for {@link ConditionalOperator}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class ConditionalOperatorUnitTests {

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectNullCondition() {
		new ConditionalOperator((Field) null, "", "");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectThenValue() {
		new ConditionalOperator(Fields.field("field"), null, "");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectOtherwiseValue() {
		new ConditionalOperator(Fields.field("field"), "", null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsEmptyFieldName() {
		newBuilder().when("");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsNullFieldName() {
		newBuilder().when((Document) null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsNullCriteriaName() {
		newBuilder().when((Criteria) null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void builderRejectsBuilderAsThenValue() {
		newBuilder().when("isYellow").then(newBuilder().when("field").then("then-value")).otherwise("otherwise");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void simpleBuilderShouldRenderCorrectly() {

		ConditionalOperator operator = newBuilder().when("isYellow").then("bright").otherwise("dark");
		Document document = operator.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document expectedCondition = new Document() //
				.append("if", "$isYellow") //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(document, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void simpleCriteriaShouldRenderCorrectly() {

		ConditionalOperator operator = newBuilder().when(Criteria.where("luminosity").gte(100)).then("bright")
				.otherwise("dark");
		Document document = operator.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document expectedCondition = new Document() //
				.append("if", new Document("$gte", Arrays.<Object> asList("$luminosity", 100))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(document, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void andCriteriaShouldRenderCorrectly() {

		ConditionalOperator operator = newBuilder() //
				.when(Criteria.where("luminosity").gte(100) //
						.andOperator(Criteria.where("hue").is(50), //
								Criteria.where("saturation").lt(11)))
				.then("bright").otherwise("dark");

		Document document = operator.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document luminosity = new Document("$gte", Arrays.<Object> asList("$luminosity", 100));
		Document hue = new Document("$eq", Arrays.<Object> asList("$hue", 50));
		Document saturation = new Document("$lt", Arrays.<Object> asList("$saturation", 11));

		Document expectedCondition = new Document() //
				.append("if", Arrays.<Object> asList(luminosity, new Document("$and", Arrays.asList(hue, saturation)))) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(document, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void twoArgsCriteriaShouldRenderCorrectly() {

		Criteria criteria = Criteria.where("luminosity").gte(100) //
				.and("saturation").and("chroma").is(200);
		ConditionalOperator operator = newBuilder().when(criteria).then("bright").otherwise("dark");

		Document document = operator.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document gte = new Document("$gte", Arrays.<Object> asList("$luminosity", 100));
		Document is = new Document("$eq", Arrays.<Object> asList("$chroma", 200));

		Document expectedCondition = new Document() //
				.append("if", Arrays.asList(gte, is)) //
				.append("then", "bright") //
				.append("else", "dark");

		assertThat(document, isBsonObject().containing("$cond", expectedCondition));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void nestedCriteriaShouldRenderCorrectly() {

		ConditionalOperator operator = newBuilder() //
				.when(Criteria.where("luminosity").gte(100)) //
				.then(newBuilder() //
						.when(Criteria.where("luminosity").gte(200)) //
						.then("verybright") //
						.otherwise("not-so-bright")) //
				.otherwise(newBuilder() //
						.when(Criteria.where("luminosity").lt(50)) //
						.then("very-dark") //
						.otherwise("not-so-dark"));

		Document document = operator.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document trueCondition = new Document() //
				.append("if", new Document("$gte", Arrays.<Object> asList("$luminosity", 200))) //
				.append("then", "verybright") //
				.append("else", "not-so-bright");

		Document falseCondition = new Document() //
				.append("if", new Document("$lt", Arrays.<Object> asList("$luminosity", 50))) //
				.append("then", "very-dark") //
				.append("else", "not-so-dark");

		assertThat(document, isBsonObject().containing("$cond.then.$cond", trueCondition));
		assertThat(document, isBsonObject().containing("$cond.else.$cond", falseCondition));
	}
}
