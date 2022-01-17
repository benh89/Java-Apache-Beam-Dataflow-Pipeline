package net.yorksolutions;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;

public class Beamflow {
    public static void main(String[] args) {
        // pipeline options for GCP
        DataflowPipelineOptions pipeline = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipeline.setRunner(DataflowRunner.class);
        pipeline.setProject("york-cdf-start");
        pipeline.setTempLocation("gs://york_temp_files/temp4");
        pipeline.setRegion("us-central1");
        pipeline.setJobName("java-final-ben-huang");


        Pipeline pl = Pipeline.create(pipeline);

        // table schemas
        TableSchema product_views = new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
                ));
        TableSchema total_sales = new TableSchema().setFields(Arrays.asList(
                        new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                        new TableFieldSchema().setName("total_sales_amount").setType("FLOAT").setMode("REQUIRED")
                ));
        TableReference _spec1 = new TableReference()
                        .setProjectId("york-cdf-start")
                        .setDatasetId("final_ben_huang")
                        .setTableId("total_no_of_product_views_jv2");

        TableReference _spec2 = new TableReference()
                        .setProjectId("york-cdf-start")
                        .setDatasetId("final_ben_huang")
                        .setTableId("total_sales_amount_jv2");

        // read from BigQuery input tables
        PCollection<TableRow> productViews =
                pl.apply(
                        "Read1 from BigQuery-join",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT cast(c.CUST_TIER_CODE as string) as cust_tier_code, cast(p.SKU as integer) as sku, count(p.EVENT_TM) as total_no_of_product_views " +
                                        "   FROM `york-cdf-start.final_input_data.product_views` p inner join " +
                                        "        `york-cdf-start.final_input_data.customers` c " +
                                        "         on c.CUSTOMER_ID = p.CUSTOMER_ID " +
                                        "   group by cast(c.CUST_TIER_CODE as string),cast(p.SKU as integer) ").usingStandardSql());
        PCollection<TableRow> totalSales =
                pl.apply(
                        "Read2 from BigQuery-join",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT cast(c.CUST_TIER_CODE as string) as cust_tier_code, o.SKU as sku, sum(o.ORDER_AMT) as total_sales_amount " +
                                        "   FROM `york-cdf-start.final_input_data.orders` o inner join " +
                                        "        `york-cdf-start.final_input_data.customers` c " +
                                        "         on c.CUSTOMER_ID = o.CUSTOMER_ID " +
                                        "   group by cast(c.CUST_TIER_CODE as string),o.SKU ").usingStandardSql());

        // writing to..
        productViews.apply(
                "Writing product-views",
                BigQueryIO.writeTableRows()
                        .to(_spec1)
                        .withSchema(product_views)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );
        totalSales.apply(
                "Writing total-sales",
                BigQueryIO.writeTableRows()
                        .to(_spec2)
                        .withSchema(total_sales)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );

        pl.run().waitUntilFinish();

    }
}
