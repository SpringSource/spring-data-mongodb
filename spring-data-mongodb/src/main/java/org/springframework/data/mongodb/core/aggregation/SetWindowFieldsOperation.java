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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 3.3
 * @see <a href=
 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/setWindowFields/">https://docs.mongodb.com/manual/reference/operator/aggregation/setWindowFields/</a>
 */
public class SetWindowFieldsOperation
		implements AggregationOperation, FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation {

	@Nullable //
	private Object partitionBy;

	@Nullable //
	private AggregationOperation sortBy;

	private WindowOutput output;

	public SetWindowFieldsOperation(@Nullable Object partitionBy, @Nullable AggregationOperation sortBy, WindowOutput output) {

		this.partitionBy = partitionBy;
		this.sortBy = sortBy;
		this.output = output;
	}

	public static SetWindowFieldsOperationBuilder builder() {
		return new SetWindowFieldsOperationBuilder();
	}

	@Override
	public ExposedFields getFields() {
		return ExposedFields.nonSynthetic(Fields.from(output.fields.toArray(new Field[0])));
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document $setWindowFields = new Document();
		if(partitionBy != null) {
			if(partitionBy instanceof AggregationExpression) {
				$setWindowFields.append("partitionBy", ((AggregationExpression) partitionBy).toDocument(context));
			} else if(partitionBy instanceof Field) {
				$setWindowFields.append("partitionBy", context.getReference((Field) partitionBy).toString());
			} else {
				$setWindowFields.append("partitionBy", partitionBy);
			}
		}

		if(sortBy != null) {
			$setWindowFields.append("sortBy", sortBy.toDocument(context).get(sortBy.getOperator()));
		}

		Document output = new Document();
		for(ComputedField field : this.output.fields) {

			Document fieldOperation = field.getWindowOperator().toDocument(context);
			if(field.window != null) {
				fieldOperation.putAll(field.window.toDocument());
			}
			output.append(field.getName(), fieldOperation);
		}
		$setWindowFields.append("output", output);

		return new Document(getOperator(), $setWindowFields);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#getOperator()
	 */
	@Override
	public String getOperator() {
		return "$setWindowFields";
	}

	public static class WindowOutput {

		private List<ComputedField> fields;

		public WindowOutput(ComputedField outputField) {

			this.fields = new ArrayList<>();
			this.fields.add(outputField);
		}

		public WindowOutput append(ComputedField field) {

			fields.add(field);
			return this;
		}
	}

	public static class ComputedField implements Field {

		private String name;
		private AggregationExpression windowOperator;

		@Nullable //
		private Window window;

		public ComputedField(String name, AggregationExpression windowOperator) {
			this(name, windowOperator, null);
		}

		public ComputedField(String name, AggregationExpression windowOperator, @Nullable Window window) {

			this.name = name;
			this.windowOperator = windowOperator;
			this.window = window;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getTarget() {
			return getName();
		}

		@Override
		public boolean isAliased() {
			return false;
		}

		public AggregationExpression getWindowOperator() {
			return windowOperator;
		}

		public Window getWindow() {
			return window;
		}
	}

	public interface Windows {

		static DocumentWindow documents(Object lower, Object upper) {
			return new DocumentWindow(lower, upper);
		}

		static RangeWindow range(Object lower, Object upper, @Nullable WindowUnit unit) {
			return new RangeWindow(lower, upper, unit);
		}

		static RangeWindowBuilder range() {
			return new RangeWindowBuilder();
		}

		static DocumentWindowBuilder documents() {
			return new DocumentWindowBuilder();
		}
	}


	public interface Window {

		Object getUpper();
		Object getLower();

		Document toDocument();
	}

	public static class RangeWindowBuilder {

		@Nullable //
		private Object upper;

		@Nullable //
		private Object lower;

		@Nullable //
		private WindowUnit unit;

		public RangeWindowBuilder upper(String upper) {

			this.upper = upper;
			return this;
		}
		public RangeWindowBuilder lower(String lower){

			this.lower = lower;
			return this;
		}
		public RangeWindowBuilder upper(Number upper) {

			this.upper = upper;
			return this;
		}

		public RangeWindowBuilder lower(Number lower){

			this.lower = lower;
			return this;
		}

		public RangeWindowBuilder fromCurrent() {
			return lower("current");
		}

		public RangeWindowBuilder fromUnbounded() {
			return lower("unbounded");
		}

		public RangeWindowBuilder toCurrent() {
			return upper("current");
		}

		public RangeWindowBuilder toUnbounded() {
			return upper("unbounded");
		}

		public RangeWindowBuilder unit(WindowUnit windowUnit) {

			this.unit = unit;
			return this;
		}

		public RangeWindow build() {
			return new RangeWindow(lower, upper, unit);
		}

	}

	public static class DocumentWindowBuilder {

		@Nullable //
		private Object upper;

		@Nullable //
		private Object lower;

		public DocumentWindowBuilder from(Number lower) {
			return lower(lower);
		}

		public DocumentWindowBuilder fromCurrent() {
			return lower("current");
		}

		public DocumentWindowBuilder fromUnbounded() {
			return lower("unbounded");
		}



		public DocumentWindowBuilder to(String upper) {

			this.upper = upper;
			return this;
		}

		DocumentWindowBuilder lower(String lower){

			this.lower = lower;
			return this;
		}
		public DocumentWindowBuilder to(Number upper) {

			this.upper = upper;
			return this;
		}

		DocumentWindowBuilder lower(Number lower) {

			this.lower = lower;
			return this;
		}


		public DocumentWindowBuilder toCurrent() {
			return to("current");
		}

		public DocumentWindowBuilder toUnbounded() {
			return to("unbounded");
		}

		public DocumentWindow build() {
			return new DocumentWindow(lower, upper);
		}
	}

	abstract static class WindowImp implements Window {

		private final Object upper;
		private final Object lower;

		protected WindowImp(Object lower, Object upper) {
			this.upper = upper;
			this.lower = lower;
		}

		@Override
		public Object getUpper() {
			return upper;
		}

		@Override
		public Object getLower() {
			return lower;
		}

	}


	public static class DocumentWindow extends WindowImp {

		DocumentWindow(Object lower, Object upper) {
			super(lower, upper);
		}

		@Override
		public Document toDocument() {
			return new Document("window", new Document("documents", Arrays.asList(getLower(), getUpper())));
		}
	}

	public static class RangeWindow extends WindowImp {

		@Nullable //
		private WindowUnit unit;

		protected RangeWindow(Object lower, Object upper, WindowUnit unit) {

			super(lower, upper);
			this.unit = unit;
		}


		@Override
		public Document toDocument() {

			Document range = new Document("range", new Object[]{getLower(), getUpper()});
			if(unit != null && !WindowUnits.DEFAULT.equals(unit)) {
				range.append("unit", unit.name().toLowerCase());
			}
			return new Document("window", range);
		}
	}

	public interface WindowUnit {
		String name();
	}

	public enum WindowUnits implements WindowUnit {
		DEFAULT,
		YEAR,
		QUARTER,
		MONTH,
		WEEK,
		DAY,
		HOUR,
		MINUTE,
		SECOND,
		MILLISECOND
	}


	public static class SetWindowFieldsOperationBuilder {

		private Object partitionBy;
		private SortOperation sortOperation;
		private WindowOutput output;

		public SetWindowFieldsOperationBuilder partitionByField(String fieldName) {
			return partitionBy(Fields.field("$" + fieldName, fieldName));
		}

		public SetWindowFieldsOperationBuilder partitionByExpression(AggregationExpression expression) {
			return partitionBy(expression);
		}

		public SetWindowFieldsOperationBuilder sortBy(String... fields) {
			return sortBy(Sort.by(fields));
		}

		public SetWindowFieldsOperationBuilder sortBy(Sort sort) {
			return sortBy(new SortOperation(sort));
		}

		public SetWindowFieldsOperationBuilder sortBy(SortOperation sort) {
			this.sortOperation = sort;
			return this;
		}

		public SetWindowFieldsOperationBuilder output(WindowOutput output) {

			this.output = output;
			return this;
		}



		public WindowChoice output(AggregationExpression expression) {

			return new WindowChoice() {

				@Nullable
				private Window window;

				@Override
				public As within(Window window) {

					this.window = window;
					return this;
				}

				@Override
				public SetWindowFieldsOperationBuilder as(String targetFieldName) {

					ComputedField computedField = new ComputedField(targetFieldName, expression, window);

					if(SetWindowFieldsOperationBuilder.this.output == null) {
						SetWindowFieldsOperationBuilder.this.output = new WindowOutput(computedField);
					} else {
						SetWindowFieldsOperationBuilder.this.output.append(computedField);
					}

					return SetWindowFieldsOperationBuilder.this;
				}
			};
		}

		public interface As {
			SetWindowFieldsOperationBuilder as(String targetFieldName);
		}

		public interface WindowChoice extends As {

			As within(Window window);

		}

		public SetWindowFieldsOperationBuilder partitionBy(Object value) {

			partitionBy = value;
			return this;
		}

		public SetWindowFieldsOperation build() {
			return new SetWindowFieldsOperation(partitionBy, sortOperation,  output);
		}
	}



}
