package com.pipeline;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class MyPipeline {

    static final TupleTag<bQTableSchema> parsedMessages = new TupleTag<bQTableSchema>() {
    };
    static final TupleTag<String> unParsedMessages = new TupleTag<String>() {
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
                                        context.output(parsedMessages, commonLog);
                                    } catch (JsonSyntaxException e) {
                                        context.output(unParsedMessages, json);
                                    }
                                }
                            })
                            .withOutputTags(parsedMessages, TupleTagList.of(unParsedMessages)));
        }
    }

    static public class ConvertToTableRow extends DoFn<bQTableSchema, TableRow> {
        @ProcessElement
        public void processing(ProcessContext context) {
            TableRow tableRow = new TableRow()
                    .set("id", Integer.parseInt(String.valueOf((context.element()))))
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
                                // Retrieve timestamp information from Pubsub Message attributes
                                .withTimestampAttribute("timestamp")
                                .fromTopic(options.getInputTopicName()))
                        .apply("ConvertMessageToCommonLog", new CommonLog());

        pubSubMessages
                .get(parsedMessages)
                        .apply("ConvertToTableRow", ParDo.of(new ConvertToTableRow()))
                                .apply("WriteToBQ", BigQueryIO.writeTableRows().to(tableSpec)
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                );

        pubSubMessages
        // Retrieve unparsed messages
                .get(unParsedMessages)
//                .apply("ConverteToString", MapElements.)
                .apply("WriteToDeadLetterTopic", PubsubIO.writeStrings().to(options.getDLQTopicName() + "/deadletter/*"));


        p.run().waitUntilFinish();
    }

}
