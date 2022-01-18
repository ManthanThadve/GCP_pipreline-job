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
import org.apache.beam.sdk.values.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MyPipeline {

    static final TupleTag<CommonLog> VALID_Messages = new TupleTag<>() {
    };
//    static final TupleTag<String> VALID_Messages = new TupleTag<>() {
//    };
    static final TupleTag<String> INVALID_Messages = new TupleTag<>() {
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


    static class JsonToCommonLog extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToCommonLog", ParDo.of(new DoFn<String, CommonLog>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    Gson gson = new Gson();
                                    try {
                                        CommonLog commonLog = gson.fromJson(json, CommonLog.class);
                                        context.output(VALID_Messages, commonLog);
                                    } catch (Exception e) {
                                        LOG.info(e.toString());
                                        context.output(INVALID_Messages, json);
                                    }
                                }
                            })
                            .withOutputTags(VALID_Messages, TupleTagList.of(INVALID_Messages)));
        }
    }

//    static class JsonToCommonLog extends DoFn<String, String> {
//        @ProcessElement
//        public void processElement(@Element String json,ProcessContext processContext) throws Exception {
//            // try {
//            //     Gson gson = new Gson();
//            //     CommonLog commonLog = gson.fromJson(json, CommonLog.class);
//            //     processContext.output(VALID_DATA,commonLog);
//            // }catch(Exception e){
//            //     LOG.info(e.toString());
//            //     processContext.output(INVALID_DATA,json);
//            // }
//            String[] arrJson=json.split(",");
//            if(arrJson.length==3) {
//                //validatios
//                if(arrJson[0].contains("id") && arrJson[1].contains("name") &&arrJson[2].contains("surname")){
//                    processContext.output(VALID_Messages,json);
//                }else{
//                    //Malformed data
//                    processContext.output(INVALID_Messages,json);
//                }
//            }else{
//                //Malformed data
//                processContext.output(INVALID_Messages,json);
//            }
//        }
//    }

//    static public class ConvertToTableRow extends DoFn<CommonLog, TableRow> {
//        @ProcessElement
//        public void processing(ProcessContext context) {
//            TableRow tableRow = new TableRow()
//                    .set("id", context.element())
//                    .set("name", context.element())
//                    .set("surname", context.element());
//
//            context.output(tableRow);
//        }
//    }

    static public class ConvertToRow extends DoFn<CommonLog, Row> {
        @ProcessElement
        public void processing(@Element CommonLog record, OutputReceiver<Row> r) {
            Row row = Row.withSchema(BQSchema).build();
            r.output(row);
        }
    }


    static public void runPipeline(MyOptions options, TableReference tableSpec) {
        Pipeline p = Pipeline.create(options);
        System.out.println("Pipeline Created");

        PCollectionTuple pubSubMessages =
                p.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .fromSubscription(options.getInputSubscriptionName()))
                        .apply("ConvertMessageToCommonLog", new JsonToCommonLog());

        pubSubMessages
                .get(VALID_Messages)

//                .apply("ConvertToTableRow", ParDo.of(new ConvertToTableRow()))

                .apply("ConvertToRow", ParDo.of(new ConvertToRow()))

                .apply("WriteToBQ", BigQueryIO.<Row>write().to(tableSpec)
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
