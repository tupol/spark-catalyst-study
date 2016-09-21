import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

/**
 *
 */
public class ComplexDataFrameExample1GeneratedClass extends GeneratedClass {

    public Object generate(Object[] references) {
        return new GeneratedIterator(references);
    }

    final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
        private Object[] references;
        private scala.collection.Iterator inputadapter_input;
        private org.apache.spark.sql.execution.metric.SQLMetric filter_numOutputRows;
        private UnsafeRow filter_result;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder filter_holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter filter_rowWriter;
        private scala.Function1 project_catalystConverter;
        private scala.Function1 project_converter;
        private scala.Function1 project_udf;
        private scala.Function1 project_catalystConverter1;
        private scala.Function1 project_converter1;
        private scala.Function1 project_udf1;
        private UnsafeRow project_result2;
        private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder project_holder;
        private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter project_rowWriter;

        public GeneratedIterator(Object[] references) {
            this.references = references;
        }

        public void init(int index, scala.collection.Iterator inputs[]) {
            partitionIndex = index;
            inputadapter_input = inputs[0];
            this.filter_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[0];
            filter_result = new UnsafeRow(2);
            this.filter_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(filter_result, 32);
            this.filter_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(filter_holder, 2);
            this.project_catalystConverter = (scala.Function1)org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$.createToCatalystConverter(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[1]).dataType());
            this.project_converter = (scala.Function1)org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$.createToScalaConverter(((org.apache.spark.sql.catalyst.expressions.Expression)(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[1]).getChildren().apply(0))).dataType());
            this.project_udf = (scala.Function1)(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[1]).userDefinedFunc());
            this.project_catalystConverter1 = (scala.Function1)org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$.createToCatalystConverter(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[2]).dataType());
            this.project_converter1 = (scala.Function1)org.apache.spark.sql.catalyst.CatalystTypeConverters$.MODULE$.createToScalaConverter(((org.apache.spark.sql.catalyst.expressions.Expression)(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[2]).getChildren().apply(0))).dataType());
            this.project_udf1 = (scala.Function1)(((org.apache.spark.sql.catalyst.expressions.ScalaUDF)references[2]).userDefinedFunc());
            project_result2 = new UnsafeRow(4);
            this.project_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(project_result2, 32);
            this.project_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(project_holder, 4);
        }

        protected void processNext() throws java.io.IOException {
            while (inputadapter_input.hasNext()) {
                InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
                int inputadapter_value = inputadapter_row.getInt(0);

                boolean filter_isNull = false;

                boolean filter_value = false;
                filter_value = inputadapter_value > 2;
                if (!filter_value) continue;

                filter_numOutputRows.add(1);

                boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
                UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
                Object project_arg = inputadapter_isNull1 ? null : project_converter.apply(inputadapter_value1);
                UTF8String project_result = (UTF8String)project_catalystConverter.apply(project_udf.apply(project_arg));

                boolean project_isNull1 = project_result == null;
                UTF8String project_value1 = null;
                if (!project_isNull1) {
                    project_value1 = project_result;
                }
                Object project_arg1 = false ? null : project_converter1.apply(inputadapter_value);
                Double project_result1 = (Double)project_catalystConverter1.apply(project_udf1.apply(project_arg1));

                boolean project_isNull3 = project_result1 == null;
                double project_value3 = -1.0;
                if (!project_isNull3) {
                    project_value3 = project_result1;
                }
                boolean project_isNull5 = false;

                boolean project_isNull7 = false;

                boolean project_isNull8 = false;

                int project_value8 = -1;
                project_value8 = inputadapter_value * inputadapter_value;

                int project_value7 = -1;
                project_value7 = project_value8 * 3;
                int project_value5 = -1;
                project_value5 = inputadapter_value + project_value7;
                project_holder.reset();

                project_rowWriter.zeroOutNullBytes();

                project_rowWriter.write(0, inputadapter_value);

                if (project_isNull1) {
                    project_rowWriter.setNullAt(1);
                } else {
                    project_rowWriter.write(1, project_value1);
                }

                if (project_isNull3) {
                    project_rowWriter.setNullAt(2);
                } else {
                    project_rowWriter.write(2, project_value3);
                }

                project_rowWriter.write(3, project_value5);
                project_result2.setTotalSize(project_holder.totalSize());
                append(project_result2);
                if (shouldStop()) return;
            }
        }
    }

}
