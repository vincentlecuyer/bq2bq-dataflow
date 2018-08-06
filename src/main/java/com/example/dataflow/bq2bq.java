

package com.example.dataflow;

import java.util.List;
import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class bq2bq {

    public static void main(String[] args) {
        
        // Set the schema for the output table in BigQuery (not necessary if you change withCreateDisposition to CREATE_NEVER on write.)
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("year").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner you wish to use.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        // Create the Pipeline object with the options we defined above (which come from arguments in this case since
        // you want to be able to publish a template.
        Pipeline p = Pipeline.create(options);
        
        // Concept #1: Apply a root transform to the pipeline; in this case, BigQueryIO.Read to read a set
        // of input text files. BigQueryIO.Read returns a PCollection of TableRows where each element is one row
        // from the BigQuery query.
       PCollection<TableRow> QueryResult = p.apply(
               "Query BigQuery"
               , BigQueryIO.readTableRows().fromQuery("SELECT year FROM [bigquery-public-data:samples.natality] LIMIT 1000")
        );
        
        // Concept #2 : Send the TableRow collection to BigQueryIO.Write (must have schema). This will write the data to the table (and create the table according ot the schema if it's not already available.)
        QueryResult.apply(
                "Write to BigQuery"
                , BigQueryIO.writeTableRows().to("transat-data:my_new_dataset.test").withSchema(schema).withWriteDisposition(WriteDisposition.WRITE_APPEND)
        );
        
        // Run the pipeline
        p.run();
        
    }
}