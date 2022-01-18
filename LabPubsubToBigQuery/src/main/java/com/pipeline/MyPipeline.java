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
	    
        /**
         *Pipeline Options.
         */
        MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("nttdata-c4e-bde");
	    
        /**
         *BigQuery Table specifications.
         */
        TableReference tableSpec = new TableReference()
                .setProjectId("nttdata-c4e-bde")
                .setDatasetId("uc1_4")
                .setTableId("account");

        runPipeline(options,tableSpec);
    }

        /**
         *Serilization Class -> tagging messages as VALID/INVALID and returns a PCollectionTuple
         */
    public static class CommonLog extends DoFn<String, bQTableSchema> {

        private static final long serialVersionUID = 1L;

        public static PCollectionTuple convert(PCollection<String> input) throws Exception {
            return input.apply("JsonToCommonLog", ParDo.of(new DoFn<String, bQTableSchema>() {
                
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

               /**
                *Schema Compatible with the already extisting BigQuery table
                */
    public static final Schema BQSchema = Schema.builder().addInt64Field("id").addStringField("name")
            .addStringField("surname").build();

    static public void runPipeline(MyOptions options, TableReference tableSpec) throws Exception{
        Pipeline p = Pipeline.create(options);
        System.out.println("Pipeline Created");
	    
	        /**
                * PCollection of Json Strings from PubSub 
                */
		PCollection<String> pubSubMessagesStrings =
                p.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .fromSubscription(options.getInputSubscriptionName()));
	    
	    	        /**
                * Converting the Json to commonLog (Serialization) , And tagging them As VALID
		*If encounter any error tag the Json String as INVALID.
                */
                PCollectionTuple pubSubMessages = CommonLog.convert(pubSubMessagesStrings);
	    
	    
	    	        /**
                * Converting VALID CommonLog to Json (Deserialization), Then Converting them to Row to write into BigQuery.
                */
        pubSubMessages
                .get(VALID_Messages)
                .apply(ParDo.of("CommonLogToJson", new DoFn<bQTableSchema, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext context)
                    {
                        Gson gObj = new Gson();
                        String jsonRecord = gObj.toJson(context.element());
                        context.output(jsonRecord);
                    }
                }))
                
                .apply("IsontoRow", JsonToRow.withSchema(BQSchema))

                .apply("WriteToBQ", BigQueryIO.<Row>write().
                                         to(tableSpec).useBeamSchema()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                );
	    	        /**
                * Writing INVALID Messages into the DEAD LETTER TOPIC
                */
        pubSubMessages
                .get(INVALID_Messages)
                .apply("WriteToDeadLetterTopic", PubsubIO.writeStrings().to(options.getDLQTopicName()));

        p.run().waitUntilFinish();
    }
}
