CREATE TABLE [dbo].[pipeline_runs] (

	[run_id] varchar(200) NOT NULL, 
	[load_dts] datetime2(6) NOT NULL, 
	[source_entity] varchar(200) NOT NULL
);