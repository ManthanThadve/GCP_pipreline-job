package com.pipeline;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyPipeline {

    static final TupleTag<bQTableSchema> VALID_Messages = new TupleTag<bQTableSchema>() {
    };
    static final TupleTag<String> INVALID_Messages = new TupleTag<String>() {
    };

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static final Schema BQSchema = Schema.builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("nttdata-c4e-bde");

        TableReference tableSpec = new TableReference()
                .setProjectId("nttdata-c4e-bde")
                .setDatasetId("uc1_4")
                .setTableId("account");

        runPipeline(options,tableSpec);
    }


    static class CommonLog extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToCommonLog", ParDo.of(new DoFn<String, bQTableSchema>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    Gson gson = new Gson();
                                    try {
                                        bQTableSchema commonLog = gson.fromJson(json, bQTableSchema.class);
                                        context.output(VALID_Messages, commonLog);
                                    } catch (JsonSyntaxException e) {
                                        context.output(INVALID_Messages, json);
                                    }
                                }
                            })
                            .withOutputTags(VALID_Messages, TupleTagList.of(INVALID_Messages)));
        }
    }

    static public class ConvertToTableRow extends DoFn<bQTableSchema, TableRow> {
        @ProcessElement
        public void processing(ProcessContext context) {
            TableRow tableRow = new TableRow()
                    .set("id", context.element())
                    .set("name", context.element())
                    .set("surname", context.element());

            context.output(tableRow);
        }
    }

    static public void runPipeline(MyOptions options, TableReference tableSpec) {
        Pipeline p = Pipeline.create(options);
        System.out.println("Pipeline Created");

        PCollectionTuple pubSubMessages =
                p.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .withTimestampAttribute("timestamp")
                                .fromSubscription(options.getInputSubscriptionName()))
                        .apply("ConvertMessageToCommonLog", new CommonLog());

        pubSubMessages
                .get(VALID_Messages)
                .setRowSchema(BQSchema)

                .apply("ConvertToTableRow", ParDo.of(new ConvertToTableRow()))

                .apply("WriteToBQ", BigQueryIO.writeTableRows().to(tableSpec)
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                );

        pubSubMessages
        // Retrieve unparsed messages
                .get(INVALID_Messages)
                .apply("WriteToDeadLetterTopic", PubsubIO.writeStrings().to(options.getDLQTopicName()));


        p.run().waitUntilFinish();
    }

}
