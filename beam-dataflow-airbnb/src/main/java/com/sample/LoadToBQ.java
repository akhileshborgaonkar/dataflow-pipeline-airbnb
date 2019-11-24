package com.sample;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

/*
 **	@author: Akhilesh Borgaonkar (borgaonkar.akhilesh@gmail.com)
 */

public class LoadToBQ {

	private static final Logger LOGGER = LoggerFactory.getLogger(LoadToBQ.class);
	private static String HEADERS = "id,name,host_id,host_name,neighbourhood_group,"
			+ "neighbourhood,latitude,longitude,room_type,price,minimum_nights,number_of_reviews,"
			+ "last_review,reviews_per_month,calculated_host_listings_count,availability_365";

	private static class FormatForBigquery extends DoFn<String, TableRow> {

		/**
		 * 	Functions to handle the parsing of input data-set (Module 1)
		 */
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow row = new TableRow();												//native row format of BigQuery used for transformation
			String[] parts = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");	//splitting the fields in input file by comma delimiter outside quotes
			String[] COLUMNS = HEADERS.split(",");										//get the column names

			if (!c.element().contains(HEADERS) && parts.length == 16) {					//check if current row is not header and contains all fields to avoid null fields in BigQuery table
				for (int i = 0; i < parts.length; i++) {
					row.set(COLUMNS[i], parts[i]);										//setting the field values to column attribute in the row object
				}
				c.output(row);															//Adding the row object to main output
			}
		}

		static TableSchema getSchema() {												//Define the BigQuery schema w.r.t. with the fields in input file
			List<TableFieldSchema> fields = new ArrayList<>();
			fields.add(new TableFieldSchema().setName("id").setType("STRING"));
			fields.add(new TableFieldSchema().setName("name").setType("STRING"));
			fields.add(new TableFieldSchema().setName("host_id").setType("STRING"));
			fields.add(new TableFieldSchema().setName("host_name").setType("STRING"));
			fields.add(new TableFieldSchema().setName("neighbourhood_group").setType("STRING"));
			fields.add(new TableFieldSchema().setName("neighbourhood").setType("STRING"));
			fields.add(new TableFieldSchema().setName("latitude").setType("STRING"));
			fields.add(new TableFieldSchema().setName("longitude").setType("STRING"));
			fields.add(new TableFieldSchema().setName("room_type").setType("STRING"));
			fields.add(new TableFieldSchema().setName("price").setType("STRING"));
			fields.add(new TableFieldSchema().setName("minimum_nights").setType("STRING"));
			fields.add(new TableFieldSchema().setName("number_of_reviews").setType("STRING"));
			fields.add(new TableFieldSchema().setName("last_review").setType("STRING"));
			fields.add(new TableFieldSchema().setName("reviews_per_month").setType("STRING"));
			fields.add(new TableFieldSchema().setName("calculated_host_listings_count").setType("STRING"));
			fields.add(new TableFieldSchema().setName("availability_365").setType("STRING"));

			return new TableSchema().setFields(fields);
		}
	}

	private static class ExtractNeighborhoods extends DoFn<String, String> {

		/**
		 *	Functions to handle the neighborhood aggregations (Module 2)
		 */
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(@Element String raw, OutputReceiver<String> receiver) {

			String[] parts = raw.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");				//splitting the fields in input file by comma delimiter outside quotes
			if (!raw.contains(HEADERS) && parts.length == 16 && !parts[5].isEmpty())		//check if current row is not header, contains all fields & valid neighbourhood field
				receiver.output(parts[5]);													//Adding neighborhood field to output receiver for the function call
		}

		static TableSchema getSchema() {													//Define the BigQuery schema w.r.t. with the fields in input file
			List<TableFieldSchema> fields = new ArrayList<>();
			fields.add(new TableFieldSchema().setName("neighbourhood").setType("STRING"));	
			fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));

			return new TableSchema().setFields(fields);
		}

		static class AggrHelper extends SimpleFunction<KV<String, Long>, TableRow> {		
			/**
			 *	helper function to transform the key value pair to BigQuery row format
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public TableRow apply(KV<String, Long> input) {
				TableRow row = new TableRow();
				row.set("neighbourhood", input.getKey());			//get every neighbourhood key from previous collection
				row.set("count", input.getValue().intValue());		//get count of resp. neighbourhood
				return row;
			}
		}
	}



	public static void main(String[] args) throws Throwable {

		String sourceFilePath = "gs://nyc-airbnb/AB_NYC_2019.csv";		//input file path
		String tempLocationPath = "gs://nyc-airbnb/tmp/";				//metadata storage path

		TableReference tableRef = new TableReference();
		tableRef.setProjectId("rare-scout-258800");						//native GCP project ID
		tableRef.setDatasetId("working");								//BigQuery target data-set
		tableRef.setTableId("airbnb_nyc_dataset");						//BigQuery target table to store the parsed records from input dataset

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();	//using PipelineOptions to customize the dataflowRunner pipeline configurations 
		options.setTempLocation(tempLocationPath);
		options.setJobName("parse-airbnb-dataset");						//set the job name to track the progress and identify the process

		Pipeline p = Pipeline.create(options);							//using Pipeline to manage the DAGs generated as the steps in loading to BigQuery process

		PCollection<String> readCollection = p.apply("Read input CSV dataset", TextIO.read().from(sourceFilePath));		//Reading the file into in-memory collection

		/*
		 * Module 1: Saving the parsed dataset into BigQuery table
		 */
		readCollection.apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))		//Transforming the output to BigQuery compatible row format
			.apply("Write into BigQuery", BigQueryIO.writeTableRows()								//Writing to the BigQuery table
					.to(tableRef).withSchema(FormatForBigquery.getSchema())							//fetching custom schema to align with table schema
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)		//if table doesn't exist then create new table
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));		//if the table already exists then over-write the contents of table

		/* 
		 * Module 2: Aggregating the neighbourhood field in dataset to get Group By counts
		 */
		PCollection<String> neighborhoods = readCollection.apply("Getting all Neighbourhoods values", ParDo.of(new ExtractNeighborhoods()));	//Extracting the neighborhood field and generating another PCollection
		PCollection<KV<String, Long>> neighborhoodCounts = neighborhoods.apply(Count.perElement());			//Performing Group By aggregation to get count of records for every neighbourhood entity

		TableReference tableRef2 = new TableReference();
		tableRef2.setProjectId("rare-scout-258800");						//native GCP project ID
		tableRef2.setDatasetId("working");									//BigQuery target data-set
		tableRef2.setTableId("nyc_neighbourhoods");							//BigQuery target table to store neighbourhood aggregation results

		neighborhoodCounts.apply("Convert to BigQuery TableRow", MapElements.via(new ExtractNeighborhoods.AggrHelper()))
			.apply("Write into BigQuery", BigQueryIO.writeTableRows()								//Writing to the BigQuery table
					.to(tableRef2).withSchema(ExtractNeighborhoods.getSchema())						//fetching custom schema to align with table schema
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)		//if table doesn't exist then create new table
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));		//if the table already exists then over-write the contents of table

		p.run().waitUntilFinish();											//wait the process execution till the final state is reached

	}
}
