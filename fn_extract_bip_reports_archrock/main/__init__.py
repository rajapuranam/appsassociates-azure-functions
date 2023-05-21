from io import BytesIO
import pandas as pd
import logging
from zeep import Client
import pandas as pd
import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from bs4 import BeautifulSoup

def get_current_datetime():
	'''
		Returns current timestamp in form of yyyymmdd_hhmmss
	'''
	now = datetime.now()
	dt_string = now.strftime("%Y%m%d_%H%M%S")
	return dt_string

def get_secret_details(secretname):
	'''
		Returns secret value (password) by taking secret name.
	'''
	keyVaultName = "<Key Vault Name>"
	KVUri = f"https://{keyVaultName}.vault.azure.net"
	credential = DefaultAzureCredential()
	client = SecretClient(vault_url=KVUri, credential=credential)
	retrieved_secret = client.get_secret(secretname)
	return retrieved_secret.value

def load_parquet_file(folder_name, parquet_file):
	'''
		Loads parquet file into Azure Storage by taking folder name and parquet file as parameters.
	'''
	logging.info('\nload_parquet_file() called.')
	folder_name = folder_name.lower()
	now = get_current_datetime()
	parquet_file_name = f"{folder_name}_{now}.parquet"
	connection_string = "<azure_storage_connectiuon_string>"
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	container_name = "<container_name>"
	blob_name = f"<blob_folder_path>/{folder_name}/{parquet_file_name}"
	blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
	blob_client.upload_blob(parquet_file, overwrite=True)
	logging.info(f'Parquet file loaded to {blob_name} location.')

def login(hostname, userid, password):
	'''
		Logins by taking hostname, user credentials as parameters.
		Returns Session Token
	'''
	logging.info('login() called.\n')
	SecurityService = f'https://{hostname}/xmlpserver/services/v2/SecurityService?wsdl'
	SecurityService = Client(SecurityService)
	loginRequest = {'userID': userid, 'password': password}
	session_token = SecurityService.service.login(**loginRequest)
	logging.info('Login Successful...')
	return session_token

def logout(hostname, session_token):
	'''
		Logout from current session. Takes hostname and session token as parameter.
	'''
	logging.info('logout() called.\n')
	SecurityService = f'https://{hostname}/xmlpserver/services/v2/SecurityService?wsdl'
	logout = {'bipSessionToken': session_token}
	SecurityService = Client(SecurityService)
	SecurityService.service.logout(**logout)
	logging.info('Logout Successful...')

def get_parameters_object(parameter_list):
	'''
		Generates parameters object and returns it.
	'''
	print("\nget_parameters_object() called...")
	parameter_list = parameter_list.split('^')
	paramlist = []
	for param in parameter_list:
		param_label, param_value = param.split('~')
		paramdict = {
			'label': f'{param_label}',
			'multiValuesAllowed': False,
			'name': f'{param_label}',
			'refreshParamOnChange': False,
			'selectAll': False,
			'templateParam': False,
			'useNullForAll': True,
			'values': {'item': [f'{param_value}']}
		}
		paramlist.append(paramdict)
	
	ArrayOfParamNameValue={'item':paramlist}
	ParamNameValues= {'listOfParamNameValues':ArrayOfParamNameValue}
	return ParamNameValues

def parse_html_to_dataframe(rtf_report_output):
	'''
		Parses HTML data to Pandas Dataframe.
		Returns dataframe.
	'''
	soup = BeautifulSoup(rtf_report_output, "html.parser")
	table_data = soup.find_all('table')[-1]
	table_records = table_data.findChildren(['tr'])
	is_first_row = True
	data = []
	columns = []
	for row in table_records:
		curr_data = []
		if is_first_row:
			cells = row.findChildren('td')
			for cell in cells:
				columns.append(cell.getText().strip())
			is_first_row = False
		else:
			cells = row.findChildren('td')
			for cell in cells:
				curr_data.append(cell.getText().strip())
			data.append(curr_data.copy())
	df = pd.DataFrame(data, columns=columns)
	return df

def extract_bip_report(hostname, userid, password, reportpath, reportname, report_format, bronze_folder_name, parameter_list):
	'''
		Extracts BIP reports to Azure Storage by following below steps.
		- Login to account
		- Generates Session Token
		- Generates parameter object if there are any parameters to the BIP reports.
		- Extracts BIP reports 
		- Converts the report into parquet file.
		- Loads the parquet file into Azure Storage (Bronze layer).
		- Logout current session.
	'''
	logging.info('extract_bip_report() called.\n')
	if report_format.lower() == 'rtf':
		report_format_modified = 'RTF'
	elif report_format.lower() == 'csv':
		report_format_modified = 'csv'
	else:
		raise Exception('Invalid report found selected.')
	# Logging request body parameters
	logging.info(f'\n\nbronze_folder_name: {bronze_folder_name}')
	logging.info(f'Parameter List: {parameter_list}')
	logging.info(f'Report Path: {reportpath}')
	logging.info(f'Report Name: {reportname}')
	logging.info(f'Report Format: {report_format}')
	logging.info(f'Report Format: {report_format_modified}\n\n')
	
	session_token = login(hostname, userid, password)
	logging.info(f'Session Token: {session_token}')

	if session_token is None:
		raise Exception('Session not created.')

	ReportService = f'https://{hostname}/xmlpserver/services/v2/ReportService?wsdl'
	report_url = f'{reportpath}/{reportname}.xdo'
	logging.info(f'Report URL: {report_url}')
	
	if parameter_list != 'NA':
		ParamNameValues = get_parameters_object(parameter_list)
		reportRequest = {
			'attributeFormat': report_format_modified,
			'reportAbsolutePath': report_url,
			'byPassCache': True,
			'attributeLocale': 'en-US',
			'sizeOfDataChunkDownload': -1,
			'flattenXML': True,
			'parameterNameValues':ParamNameValues
		}
	else:
		reportRequest = {
			'attributeFormat': report_format_modified,
			'reportAbsolutePath': report_url,
			'byPassCache': True,
			'attributeLocale': 'en-US',
			'sizeOfDataChunkDownload': -1,
			'flattenXML': True,
		}
	
	logging.info(f'\nreportRequest:\n{reportRequest}')

	runReportRequest = {
		'reportRequest': reportRequest, 
		'bipSessionToken': session_token
	}
	ReportService = Client(ReportService)
	logging.info(f'Report Service: {ReportService}')
	Represponse = ReportService.service.runReportInSession(**runReportRequest)
	logging.info(f'Report Response: {Represponse}')
	
	if Represponse is None:
		logout(hostname, session_token)
		raise Exception('Report Response is None.')
		
	reportbyte = Represponse['reportBytes']
	logging.info(f'Type of reportbyte: {type(reportbyte)}')
	result_data = reportbyte.decode('utf-8',errors='ignore')
	result = bytes(result_data, 'utf-8')
	logging.info(f'Type of result: {type(result)}')

	if report_format_modified.lower() == 'csv':
		df = pd.read_csv(BytesIO(result), on_bad_lines='skip')
	elif report_format_modified.lower() == 'rtf':
		df = parse_html_to_dataframe(result)
	
	parquet_file = BytesIO()
	df.to_parquet(parquet_file, engine = 'pyarrow')
	parquet_file.seek(0)
	# logging.info(f'parquet_file: {parquet_file}')
	load_parquet_file(bronze_folder_name, parquet_file)
	logout(hostname, session_token)


def main(req: func.HttpRequest) -> func.HttpResponse:
	'''
		Acts as a Main function that extracts BIP reports and loads into Azure storage as parquet files.
	'''
	logging.info('Python HTTP trigger function processed a request.')
	hostname = '<hostname>.oraclecloud.com'
	try:
		# Taking request body and parsing to variables
		req_body = req.get_json()
		secret_name = req_body.get('secret_name')
		report_path = req_body.get('report_path')
		report_name = req_body.get('report_name')
		report_format = req_body.get('report_format')
		bronze_folder_name = req_body.get('bronze_folder_name')
		parameter_list = req_body.get('parameter_list')		
		userid = '<username>'
		# Retriving password from Azure Key vault
		password = get_secret_details(secret_name)

		# Calling extract_bip_report function by passing required parameters
		extract_bip_report(
			hostname, userid, password, 
			report_path, report_name, report_format, 
			bronze_folder_name, parameter_list
		)
		
	except Exception as ex:
		return func.HttpResponse(f"ERROR_FOUND {ex} - {ex.__doc__}", status_code = 500)
	else:
		return func.HttpResponse(f"\nBIP Report extracted successfully.\n")