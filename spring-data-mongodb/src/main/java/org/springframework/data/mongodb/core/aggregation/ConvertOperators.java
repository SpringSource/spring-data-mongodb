/*
 * Copyright 2018 the original author or authors.
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

import java.util.Collections;

import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal convert} aggregation operations.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class ConvertOperators {

	/**
	 * Take the value referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static ConvertOperatorFactory valueOf(String fieldReference) {
		return new ConvertOperatorFactory(fieldReference);
	}

	/**
	 * Take the value provided by the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static ConvertOperatorFactory valueOf(AggregationExpression expression) {
		return new ConvertOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class ConvertOperatorFactory {

		private final @Nullable String fieldReference;
		private final @Nullable AggregationExpression expression;

		/**
		 * Creates new {@link ConvertOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public ConvertOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ConvertOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ConvertOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given identifier. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param stringTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(String stringTypeIdentifier) {
			return createConvert().to(stringTypeIdentifier);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given identifier. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param numericTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(int numericTypeIdentifier) {
			return createConvert().to(numericTypeIdentifier);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given type. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(Type type) {
			return createConvert().to(type);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the value of the given {@link Field field reference}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertToTypeOf(String fieldReference) {
			return createConvert().toTypeOf(fieldReference);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given {@link AggregationExpression expression}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertToTypeOf(AggregationExpression expression) {
			return createConvert().toTypeOf(expression);
		}

		/**
		 * Creates new {@link ToBool aggregation expression} for {@code $toBool} that converts a value to boolean. Shorthand
		 * for {@link #convertTo(String) #convertTo("bool")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToBool}.
		 */
		public ToBool convertToBoolean() {
			return ToBool.toBoolean(valueObject());
		}

		private Convert createConvert() {
			return usesFieldRef() ? Convert.convertValueOf(fieldReference) : Convert.convertValueOf(expression);
		}

		private Object valueObject() {
			return usesFieldRef() ? Fields.field(fieldReference) : expression;
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $convert} that converts a value to a specified type. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 * "https://docs.mongodb.com/manual/reference/operator/aggregation/convert/">https://docs.mongodb.com/manual/reference/operator/aggregation/convert/</a>
	 * @since 2.1
	 */
	public static class Convert extends AbstractAggregationExpression {

		private Convert(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Convert} using the given value for the {@literal input} attribute.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValue(Object value) {
			return new Convert(Collections.singletonMap("input", value));
		}

		/**
		 * Creates new {@link Convert} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValueOf(String fieldReference) {
			return convertValue(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Convert} using the result of the provided {@link AggregationExpression expression} as
		 * {@literal input} value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValueOf(AggregationExpression expression) {
			return convertValue(expression);
		}

		/**
		 * Specify the conversion target type via its {@link String} representation.
		 * <ul>
		 * <li>double</li>
		 * <li>string</li>
		 * <li>objectId</li>
		 * <li>bool</li>
		 * <li>date</li>
		 * <li>int</li>
		 * <li>long</li>
		 * <li>decimal</li>
		 * </ul>
		 *
		 * @param stringTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(String stringTypeIdentifier) {
			return new Convert(append("to", stringTypeIdentifier));
		}

		/**
		 * Specify the conversion target type via its numeric representation.
		 * <dl>
		 * <dt>1</dt>
		 * <dd>double</dd>
		 * <dt>2</dt>
		 * <dd>string</li>
		 * <dt>7</dt>
		 * <dd>objectId</li>
		 * <dt>8</dt>
		 * <dd>bool</dd>
		 * <dt>9</dt>
		 * <dd>date</dd>
		 * <dt>16</dt>
		 * <dd>int</dd>
		 * <dt>18</dt>
		 * <dd>long</dd>
		 * <dt>19</dt>
		 * <dd>decimal</dd>
		 * </dl>
		 *
		 * @param numericTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(int numericTypeIdentifier) {
			return new Convert(append("to", numericTypeIdentifier));
		}

		/**
		 * Specify the conversion target type.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(Type type) {

			String typeString = Type.BOOLEAN.equals(type) ? "bool" : type.value().toString();
			return to(typeString);
		}

		/**
		 * Specify the conversion target type via the value of the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert toTypeOf(String fieldReference) {
			return new Convert(append("to", Fields.field(fieldReference)));
		}

		/**
		 * Specify the conversion target type via the value of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert toTypeOf(AggregationExpression expression) {
			return new Convert(append("to", expression));
		}

		/**
		 * Optionally specify the value to return on encountering an error during conversion.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturn(Object value) {
			return new Convert(append("onError", value));
		}

		/**
		 * Optionally specify the field holding the value to return on encountering an error during conversion.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturnValueOf(String fieldReference) {
			return onErrorReturn(Fields.field(fieldReference));
		}

		/**
		 * Optionally specify the expression to evaluate and return on encountering an error during conversion.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturnValueOf(AggregationExpression expression) {
			return onErrorReturn(expression);
		}

		/**
		 * Optionally specify the value to return when the input is {@literal null} or missing.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturn(Object value) {
			return new Convert(append("onNull", value));
		}

		/**
		 * Optionally specify the field holding the value to return when the input is {@literal null} or missing.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturnValueOf(String fieldReference) {
			return onNullReturn(Fields.field(fieldReference));
		}

		/**
		 * Optionally specify the expression to evaluate and return when the input is {@literal null} or missing.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturnValueOf(AggregationExpression expression) {
			return onNullReturn(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$convert";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toBool} that converts a value to boolean. Shorthand for
	 * {@link Convert#to(String) Convert#to("bool")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 * "https://docs.mongodb.com/manual/reference/operator/aggregation/toBool/">https://docs.mongodb.com/manual/reference/operator/aggregation/toBool/</a>
	 * @since 2.1
	 */
	public static class ToBool extends AbstractAggregationExpression {

		private ToBool(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToBool} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToBool}.
		 */
		public static ToBool toBoolean(Object value) {
			return new ToBool(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toBool";
		}
	}
}
