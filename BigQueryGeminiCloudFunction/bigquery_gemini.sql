-- Create a connection object to use in the BigQuery external table creation - Follow this article: 
-- https://cloud.google.com/bigquery/docs/create-cloud-resource-connection#create-cloud-resource-connection

-- Creates an external table (Biglake table) ): This code creates a BigLake external table named <TableName> in the <ProjectID>.<DatasetName>. External tables allow BigQuery to query data stored outside of BigQuery storage.
-- Connects to external storage: The WITH CONNECTION clause specifies a connection named <ProjectID>.<Location>.<ConnectionObjName>, which likely contains the necessary credentials and configuration to access the external data source (probably Cloud Storage in this case).
-- Defines data location and type: The OPTIONS clause provides details about the external data:
-- object_metadata="DIRECTORY" indicates that the data is organized in directories.
-- uris = ['gs://<BucketName>/*'] specifies the location of the data in a Cloud Storage bucket includes all files within that bucket using the wildcard *.
-- metadata_cache_mode="MANUAL" sets the metadata caching to manual, meaning BigQuery won't automatically refresh the metadata (information about the data).
CREATE OR REPLACE EXTERNAL TABLE 
  `<ProjectID>.<DatasetName>.<TableName>` 
WITH CONNECTION `<ProjectID>.<Location>.<ConnectionObjName>` OPTIONS ( 
    object_metadata="DIRECTORY", 
    uris = ['gs://<BucketName>/*' ], 
    metadata_cache_mode="MANUAL");

-- Refresh metadata manually (better for demo else AUTO is minimum 30 minutes)
CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE("<ProjectID>.<DatasetName>.<TableName>");


-- Create a connection object to use in the BigQuery remote function creation - Follow this article: 
-- https://cloud.google.com/bigquery/docs/remote-functions#create_a_connection

-- Creates a remote function: This code defines a new function called <FunctionName> within the <ProjectID>.<DatasetName> dataset. The REMOTE keyword indicates that this function executes code outside of BigQuery. FYI article here: https://cloud.google.com/bigquery/docs/remote-functions
-- Specifies connection: WITH CONNECTION clause specifies that the function uses a pre-defined connection named <ProjectID>.<Location>.<ConnectionObj> (eg.: project-362608.us.simple_remote_function). This connection likely holds the necessary details to connect to the external service.
-- Sets the endpoint: The OPTIONS (endpoint = ...) clause defines the URL of the external service where the function's code is actually executed. In this case, it's a Cloud Function at the provided URL.
-- Handles inputs and outputs: The function takes two arguments (http_uri and prompt both as STRING) and is expected to return a string value.
-- (Optional) Batching: The commented-out max_batching_rows option suggests the function can potentially handle batched requests, but it's currently disabled. This might be useful for processing multiple rows of data efficiently.
CREATE OR REPLACE FUNCTION
	`<ProjectID>.<DatasetName>.<FunctionName>`(http_uri STRING, prompt STRING) RETURNS string
REMOTE WITH CONNECTION `<ProjectID>.<Location>.<ConnectionObjName>`
-- OPTIONS (endpoint = 'https://<CloudFunction_Endpoint>', max_batching_rows = 10);
OPTIONS (endpoint = 'https://<CloudFunction_Endpoint>');


-- Prepares data with a CTE: The WITH accessible_url AS (...) clause defines a Common Table Expression (CTE) called accessible_url. This CTE retrieves data from a BigLake table (catalog_biglake_table) and uses EXTERNAL_OBJECT_TRANSFORM with the SIGNED_URL option to generate signed URLs for accessing the data.
-- Retrieves signed URLs: The SELECT uri, signed_url statement within the CTE selects the original URI and the generated signed URL from the BigLake table. This makes the data accessible to the remote function.
-- Calls the remote function: The main SELECT statement calls the remote function (defined above), passing the signed_url (from the CTE) and the specific prompt as arguments (example of prompt: "Describe the product in exactly 5 bullet points starting with -. Do not add any text other than the 5 bullet points description." The function is used to process or analyze the data accessible via the signed URL.
-- Constructs the output: The SELECT statement retrieves the signed_url as uri and the result of the remote function call as description, effectively combining the original URI with the generated description from the external service. This output likely provides enriched information about the data in the BigLake table.
-- Flow, aka number of inference executed by Gemini in parallel will be controlled from here (for instance with a limit 100, or by date...
with accessible_url as
(
SELECT uri, signed_url, 
FROM EXTERNAL_OBJECT_TRANSFORM(TABLE `<ProjectID>.<DatasetName>.<BigTableName>`, ['SIGNED_URL'])
)
SELECT accessible_url.signed_url as uri,
  `<ProjectID>.<DatasetName>.<FunctionName>`(accessible_url.signed_url, "<PromptHere>") as description
from 
  accessible_url
