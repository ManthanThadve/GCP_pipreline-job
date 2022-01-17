package com.pipeline;

import com.google.api.services.bigquery.model.TableReference;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyPipeline {

    static final TupleTag<bQTableSchema> VALID_Messages = new TupleTag<>() {
        private static final long serialVersionUID = 1L;
    };
    static final TupleTag<String> INVALID_Messages = new TupleTag<>() {
        private static final long serialVersionUID = 1L;
    };

    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static void main(String[] args) throws Exception {

        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("nttdata-c4e-bde");

        TableReference tableSpec = new TableReference()
                .setProjectId("nttdata-c4e-bde")
                .setDatasetId("uc1_4")
                .setTableId("account");

        runPipeline(options,tableSpec);
    }


    public static class CommonLog extends DoFn<String, bQTableSchema> {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public static PCollectionTuple convert(PCollection<String> input) throws Exception {
            return input.apply("JsonToCommonLog", ParDo.of(new DoFn<String, bQTableSchema>() {
                /**
                *
                */
                private static final long serialVersionUID = 1L;

                @ProcessElement
                public void processElement(@Element String record, ProcessContext context) {

                    try {
                        Gson gson = new Gson();
                        bQTableSchema commonLog = gson.fromJson(record, bQTableSchema.class);
                        context.output(VALID_Messages, commonLog);
                    } catch (Exception e) {
                        e.printStackTrace();
                        context.output(INVALID_Messages, record);
                    }
                }
            }).withOutputTags(VALID_Messages, TupleTagList.of(INVALID_Messages)));
        }
    }


    public static final Schema BQSchema = Schema.builder().addInt64Field("id").addStringField("name")
            .addStringField("surname").build();

    static public void runPipeline(MyOptions options, TableReference tableSpec) throws Exception{
        Pipeline p = Pipeline.create(options);
        System.out.println("Pipeline Created");

		PCollection<String> pubSubMessagesStrings =
                p.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .fromSubscription(options.getInputSubscriptionName()));
                PCollectionTuple pubSubMessages = CommonLog.convert(pubSubMessagesStrings);

        pubSubMessages
                .get(VALID_Messages).
                apply(ParDo.of(new DoFn<bQTableSchema,String>() {
                    @ProcessElement
                    public void processElement( ProcessContext context)
                    {
                        Gson g=new Gson();
                        String s=g.toJson(context.element());
                         context.output(s);
                    }
                }))
                
                .apply("jsontoRow", JsonToRow.withSchema(BQSchema))

                .apply("WriteToBQ", BigQueryIO.<Row>write().
                to(tableSpec).useBeamSchema()
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
