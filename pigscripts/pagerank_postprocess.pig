%default OUTPUT_PATH 's3n://DataOut/DocGraph/docGraph-pagerank/pagerank'

final_pageranks     = LOAD '$PAGERANKS_INPUT_PATH' USING PigStorage() AS (user: chararray, pagerank: double);
nucc_codes 			= LOAD '$NUCC_CODES_INPUT' USING PigStorage('\t') AS
						(nuccCode:chararray,
						 nuccType:chararray,
						 nuccClassification:chararray,
						 nuccSpecialty:chararray);

npiData 			= LOAD '$NPI_DATA_INPUT' USING PigStorage(',') AS
						(NPICode:chararray,
						f2:chararray,
						f3:chararray,
						f4:chararray,
						f5:chararray,
						f6:chararray,
						f7:chararray,
						f8:chararray,
						f9:chararray,
						f10:chararray,
						f11:chararray,
						f12:chararray,
						f13:chararray,
						f14:chararray,
						f15:chararray,
						f16:chararray,
						f17:chararray,
						f18:chararray,
						f19:chararray,
						f20:chararray,
						f21:chararray,
						f22:chararray,
						f23:chararray,
						f24:chararray,
						f25:chararray,
						f26:chararray,
						f27:chararray,
						f28:chararray,
						f29:chararray,
						f30:chararray,
						f31:chararray,
						f32:chararray,
						f33:chararray,
						f34:chararray,
						f35:chararray,
						f36:chararray,
						f37:chararray,
						f38:chararray,
						f39:chararray,
						f40:chararray,
						f41:chararray,
						f42:chararray,
						f43:chararray,
						f44:chararray,
						f45:chararray,
						f46:chararray,
						f47:chararray,
						f48:chararray,
						f49:chararray);
						
-- build the referred to list 
npiDoctorDetail = FOREACH npiData GENERATE REPLACE(NPICode,'\\"','') as newNPICode, REPLACE(f7, '\\"', '') as firstName, 
						REPLACE(f8, '\\"','') as middleName,
						REPLACE(f6,'\\"','') as lastName, 
						REPLACE(f5, '\\"','') as orgName,
						REPLACE(f31, '\\"','') as docCity,
						REPLACE(f32, '\\"','') as docState,
						REPLACE(f48, '\\"','') as taxonomyCode;
						
npiDocName = FOREACH npiDoctorDetail GENERATE newNPICode, taxonomyCode, 
						CONCAT(CONCAT(CONCAT(CONCAT(UPPER(firstName), ' '), UPPER(middleName)), ' '),UPPER(lastName)) as newName,
						orgName, docCity,docState;
																		
--usernames           =   LOAD '$USERNAMES_INPUT_PATH' USING PigStorage(' ') AS (user: chararray, username: chararray);
influential_doctors_jnd = JOIN npiDocName by (newNPICode), final_pageranks by (user);
influential_doctors_taxonomy_jnd = JOIN influential_doctors_jnd by (taxonomyCode), nucc_codes by nuccCode;

--joined              =   JOIN final_pageranks BY user, usernames BY user;
influential_usernames   =   FOREACH influential_doctors_taxonomy_jnd GENERATE
                               influential_doctors_jnd::npiDocName::newName AS username,
                               influential_doctors_jnd::npiDocName::orgName AS orgName,
                               influential_doctors_jnd::npiDocName::docCity AS docCity,
                               influential_doctors_jnd::npiDocName::docState AS docState,
                               influential_doctors_jnd::npiDocName::taxonomyCode AS taxonomyCode,
							   influential_doctors_jnd::final_pageranks::user AS npiCode,
							   influential_doctors_jnd::final_pageranks::pagerank AS pagerank;

--projected           =   FOREACH influential_usernames GENERATE username, pagerank;
projected			=   FOREACH influential_usernames GENERATE npiCode, orgName, username, docCity, docState, pagerank, taxonomyCode;
ordered             =   ORDER projected BY pagerank DESC;
top_users           =   LIMIT ordered $TOP_N;

rmf $OUTPUT_PATH;
STORE top_users INTO '$OUTPUT_PATH' USING PigStorage();
